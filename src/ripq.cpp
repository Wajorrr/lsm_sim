#include "ripq.h"

static bool file_defined = false;
static double lastRequest = 0;

ripq::ripq(stats stat, size_t block_size, size_t num_sections, size_t flash_size)
    : Policy{stat}, sections{}, num_sections(num_sections),
      section_size{}, warmup{}, map{}, out{}
{
  assert(block_size);
  assert(num_sections && flash_size);
  section_size = flash_size / num_sections; // 段大小
  assert(section_size);

  for (uint32_t i = 0; i < num_sections; i++)
  {
    // 初始化段
    section_ptr curr_section(new section(i, stat));
    // 将段加入到段数组中
    sections.push_back(curr_section);
    global_bid = 0;

    // initiate active blocks for each section
    // 每个段一个开放物理块和一个开放逻辑块
    block_ptr new_phy_block(new block(sections[i], false));
    sections[i]->active_phy_block = new_phy_block;

    block_ptr new_vir_block(new block(sections[i], true));
    sections[i]->active_vir_block = new_vir_block;
  }
}

ripq::~ripq()
{
}

// Simply returns the current number of bytes cached.
size_t ripq::get_bytes_cached() const { return stat.bytes_cached; }

// Public accessors for hits and accesses.
size_t ripq::get_hits() { return stat.hits; }
size_t ripq::get_accs() { return stat.accesses; }

// add new Request to section_id
void ripq::add(const Request *r, int section_id)
{

  // First make enough space in the target active physical block, then write
  section_ptr target_section = sections[section_id];
  // 开放块空间不够了
  while (target_section->active_phy_block->filled_bytes + r->size() > stat.block_size)
  {
    // Seal active blocks and write to flash
    // We increase the writes stat by block_size, but write to flash only the real block size
    assert((uint32_t)r->size() < stat.block_size);
    if (!warmup)
      stat.flash_bytes_written += stat.block_size;
    // 关闭当前开放物理块和逻辑块
    target_section->seal_phy_block();
    target_section->seal_vir_block();
    // 将段中的块进行迁移，保证段中的块不超过段的大小
    balance(section_id);
    // 若最后一个段中的块超过段的大小，则驱逐
    while (sections[num_sections - 1]->filled_bytes > section_size)
    {
      evict();
    }
  }
  // 将新请求加入到目标段中
  item_ptr new_item = target_section->add(r);
  // 请求中的item id到item的映射
  map[r->kid] = new_item;
}

// add item to the active virtual block of section_id
void ripq::add_virtual(item_ptr it, int section_id)
{
  section_ptr target_section = sections[section_id];

  // We first add the item to the virtual active block, then balance and evict. We
  // use this order to avoid the case where we first make enough space - but then
  // may evict an item which has no valid virtual block yet (i.e. we evict the
  // pyshical block of the item before allocating a new virtual blockS
  target_section->add_virtual(it);
  // 当前开放逻辑块已满，更新开放逻辑块，并进行驱逐
  while (target_section->active_vir_block->filled_bytes /* + it->req.size()*/ > stat.block_size)
  {
    assert((uint32_t)it->req.size() < stat.block_size);
    target_section->seal_vir_block();
    // 块重平衡
    balance(section_id);
    // 最后一个段进行块驱逐
    while (sections[num_sections - 1]->filled_bytes > section_size)
    {
      evict();
    }
  }
}

void ripq::balance(int start)
{
  // 遍历段，对超出容量限制的段中的块进行迁移
  for (uint32_t i = start; i < num_sections - 1; i++)
  {
    while (sections[i]->filled_bytes > section_size)
    {
      ripq::block_ptr evicted_block = sections[i]->evict_block();
      assert(evicted_block);
      sections[i + 1]->add_block(evicted_block);
    }
  }
}

// 总缓存字节数
int ripq::section::count_filled_bytes()
{
  int count = 0;
  for (block_list::const_iterator it = blocks.begin(); it != blocks.end(); it++)
  {
    if ((*it)->active)
      continue;
    count += (*it)->filled_bytes;
  }
  return count;
}

// 缓存驱逐，选取末尾段，驱逐其末尾块
void ripq::evict()
{
  /*  for (uint32_t i = 0; i < num_sections; i++) {
      assert(sections[i]->count_filled_bytes() == (int)sections[i]->filled_bytes);
    }*/
  // 末尾段
  section_ptr tail_section = sections.back();
  assert(!tail_section->blocks.empty());

  // We first have to evict the block from the section, because the reallocations might evict blocks too
  // 末尾段驱逐其末尾块
  block_ptr evicted_block = tail_section->evict_block();
  assert(!evicted_block->active);
  assert(!evicted_block->is_virtual || evicted_block->num_items == 0);
  assert(stat.bytes_cached > evicted_block->filled_bytes);
  // 更新总缓存字节数
  stat.bytes_cached -= evicted_block->filled_bytes;

  // Remove all items from their corresponding virtual blocks before reallocating them to their new sections. Otherwise we get corner cases where we evict virtual blocks with size>0
  // 遍历被驱逐块中的对象，若对象的逻辑块不等于物理块，说明对象无效或者需要迁移，则从原逻辑块中移除
  for (item_list::iterator iter = evicted_block->items.begin(); iter != evicted_block->items.end(); iter++)
  {
    if ((*iter)->virtual_block != (*iter)->physical_block)
    {
      block_ptr virtual_block = (*iter)->virtual_block;
      virtual_block->remove(*iter);
    }
  }

  // Go through all items of the evcited block (last block in tail section), reallocate new items
  // in other sections according to their virtual block
  // 遍历被驱逐块的所有对象
  // 再次遍历被驱逐块中对象，将有效对象驱逐，无效对象删除，需要迁移的对象迁移
  while (!evicted_block->items.empty())
  {
    if (evicted_block->is_virtual)
      break;

    // 被驱逐块的末尾对象
    item_ptr evicted_item = evicted_block->items.back();
    assert(evicted_item->physical_block == evicted_block);

    // 被驱逐对象的所属逻辑块
    block_ptr virtual_block = evicted_item->virtual_block;
    assert(!evicted_block->active);

    // First pop the item out of the block
    // 将当前对象从被驱逐块的对象列表中删除
    evicted_block->items.pop_back();

    // Item can become a ghost if the key was updated with a new a value
    // 若当前对象不是无效对象，删除索引
    // 如果是无效对象，说明当前对象被更新过，不需要处理，跳过即可
    if (!evicted_item->is_ghost)
      map.erase(evicted_item->req.kid);

    // Check if we should reallocate in another section
    // 若当前对象的逻辑块和物理块不是一个块，说明当前对象被访问过
    // 访问时只更新了逻辑块，物理块还是原来的块
    // 则现在进行驱逐时需要将当前对象重新分配到其逻辑块所在的段的物理块中
    if (virtual_block != evicted_item->physical_block)
    { // reallocate in flash
      assert(!evicted_item->physical_block->is_virtual);
      assert(!evicted_item->is_ghost);
      // 将当前对象更新到其当前逻辑块所属的段，即添加到该段的物理块中
      add(&evicted_item->req, virtual_block->s->id);
      /*    for (uint32_t i = 0; i < num_sections; i++) {
              assert(sections[i]->count_filled_bytes() == (int)sections[i]->filled_bytes);
            }*/
    }
  }
  // 因为有重新插入，需要重新平衡
  balance();
}

size_t ripq::process_request(const Request *r, bool warmup)
{
  assert(r->size() > 0);
  lastRequest = r->time;
  this->warmup = warmup;
  if (!warmup)
    ++stat.accesses;

  // 查找请求对应的item
  auto it = map.find(r->kid);

  if (it != map.end()) // 找到了
  {
    // 请求对应的item
    auto item_it = it->second;

    // 若请求大小和item大小相同，即不需要更新
    if (item_it->req.size() == r->size())
    {
      // HIT

      if (!warmup)
        ++stat.hits;

      // update virtual block
      // item所在的逻辑块
      section_ptr s = item_it->virtual_block->s;
      // 新段为当前段的前一个段
      int new_section_id = (s->id > 0) ? (s->id - 1) : s->id;
      // 将item从原逻辑块中删除
      item_it->virtual_block->remove(item_it);
      // 若item所在的物理块是开放块，将item从其中删除，然后重新插入到新段的开放物理块中
      // 开放块是默认在dram中维护，还未写入flash，因此可以直接删除
      if (item_it->physical_block->active)
      {
        assert(item_it->physical_block == item_it->virtual_block);
        block_ptr active_block = item_it->physical_block;
        item_list::iterator iter = std::find(active_block->items.begin(), active_block->items.end(), item_it);
        assert(iter != active_block->items.end());
        active_block->items.erase(iter); // 从开放块中删除
        map.erase(r->kid);               // 删除索引
        add(r, new_section_id);          // 重新插入到新段的开放物理块中
      }
      else // 若item所在的块是已关闭的块，则只更新item所在的逻辑块
      {
        add_virtual(item_it, new_section_id);
      }

      assert(item_it->virtual_block->s->id <= item_it->physical_block->s->id);
      return 1;
    }
    else // 请求大小和item大小不同，对象被修改了，需要更新
    {
      // UPDATE
      // 从原逻辑块中删除
      item_it->virtual_block->remove(item_it);
      // 将逻辑块设置为其物理块
      item_it->virtual_block = item_it->physical_block;
      // 设置为无效
      item_it->is_ghost = true;
      // 删除索引
      map.erase(r->kid);
      assert(item_it->virtual_block->s->id <= item_it->physical_block->s->id);
      assert(stat.bytes_cached > (uint32_t)item_it->req.size());
      // 更新缓存字节数
      stat.bytes_cached -= (uint32_t)item_it->req.size();
    }
  }
  else // 没找到，未命中
  {
    // MISS
  }
  // 插入新请求
  // 更新缓存字节数
  stat.bytes_cached += (uint32_t)r->size();
  if (!warmup)
  {
    stat.missed_bytes += (size_t)r->size();
  }
  // 新请求加入到最后一个段中，segmented lru
  add(r, num_sections - 1);
  return PROC_MISS;
}

ripq::item_ptr ripq::block::add(const Request *req)
{
  assert(active);
  // 块中的缓存字节数
  filled_bytes += req->size();
  // 块中的对象总数
  num_items++;
  item_ptr new_item(new item(*req, shared_from_this())); // Items are added also to virtual sections for debug purposes
  // 当前对象加入到块对象列表头部
  items.push_front(new_item);
  return items.front();
}

// 删除当前块中的指定对象
void ripq::block::remove(item_ptr victim)
{
  //  assert(s->count_filled_bytes() == (int)s->filled_bytes);
  assert((int)filled_bytes >= (int)victim->req.size());
  assert(num_items > 0);
  // 更新块中的缓存字节数
  filled_bytes -= victim->req.size();
  // 更新块中的对象数
  num_items -= 1;
  // 若为逻辑块，在对象列表中删除当前对象
  if (is_virtual)
  {
    item_list::iterator iter = std::find(items.begin(), items.end(), victim);
    assert(iter != items.end());
    items.erase(iter);
  }
  // 若不为开放块，则需要更新所属段的缓存字节数(开放块的缓存字节数还没被计入所属段)
  if (!active)
  { // decrease section size only if the Request belongs to a sealed block
    assert((int)s->filled_bytes >= (int)victim->req.size());
    // 更新当前块所属段的缓存字节数
    s->filled_bytes -= victim->req.size();
  }
}

// 关闭当前的开放物理块，并打开一个新的物理块
void ripq::section::seal_phy_block()
{
  active_phy_block->active = false;
  filled_bytes += active_phy_block->filled_bytes; // We count the block filled bytes only when it becomes sealed
  block_ptr new_phy_block(new block(shared_from_this(), false));
  blocks.push_front(active_phy_block);
  active_phy_block = new_phy_block;
}

// 关闭当前的开放逻辑块，并打开一个新的逻辑块
void ripq::section::seal_vir_block()
{
  active_vir_block->active = false;
  block_ptr new_vir_block(new block(shared_from_this(), true));
  filled_bytes += active_vir_block->filled_bytes;
  blocks.push_front(active_vir_block);
  active_vir_block = new_vir_block;
}

// 向当前段中添加请求，直接添加到当前开放物理块中
ripq::item_ptr ripq::section::add(const Request *req)
{
  // If physical block is big enough, seal it together with the virtual block and commit to section block list
  assert(active_phy_block && active_vir_block && active_vir_block->is_virtual);
  assert(active_phy_block->filled_bytes <= stat.block_size);
  assert(active_phy_block->filled_bytes + (uint32_t)req->size() <= stat.block_size);

  // Add Request to physical block
  return active_phy_block->add(req);
}

// 将当前项加入到当前段的开放逻辑块中
void ripq::section::add_virtual(item_ptr it)
{
  active_vir_block->filled_bytes += it->req.size();
  active_vir_block->num_items++;
  active_vir_block->items.push_front(it); // Items are added also to virtual sections for debug purposes
  it->virtual_block = active_vir_block;
  //  assert(count_filled_bytes() == (int)filled_bytes);
}

// 将块加入到当前段中
void ripq::section::add_block(block_ptr new_block)
{
  assert(!new_block->active);
  // 更新当前段的缓存字节数
  filled_bytes += new_block->filled_bytes;
  // 更新当前块所属的段
  new_block->s = shared_from_this();
  // 将当前块加入当前段的块列表
  blocks.push_front(new_block);
}

// 当前段驱逐末尾块
ripq::block_ptr ripq::section::evict_block()
{
  assert(!blocks.empty());
  // 末尾块
  block_ptr last_block = blocks.back();
  assert(!last_block->active);
  assert(filled_bytes >= last_block->filled_bytes);
  // 更新当前段的缓存字节数
  filled_bytes -= last_block->filled_bytes;
  blocks.pop_back();
  return last_block;
}

void ripq::dump_stats(void)
{
  std::string filename{stat.policy + "-block_size" + std::to_string(stat.block_size) + "-flash_size" + std::to_string(stat.flash_size) + "-num_sections" + std::to_string(stat.num_sections)};
  if (!file_defined)
  {
    out.open(filename);
    file_defined = true;
  }
  out << "Last Request was at :" << lastRequest << std::endl;
  stat.dump(out);
  out << std::endl;
}
