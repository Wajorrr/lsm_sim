#include "ripq_shield.h"

ripq_shield::ripq_shield(stats stat, size_t block_size, size_t num_sections, size_t dram_size, int num_dsections, size_t flash_size)
    : ripq(stat, block_size, num_sections, flash_size), dsections{}, num_dsections(num_dsections), dsection_size{}, dram_map{}
{
  // 段大小
  dsection_size = dram_size / num_dsections;
  assert(dsection_size && num_dsections && flash_size);

  // 初始化段
  for (int i = 0; i < num_dsections; i++)
  {
    dsection_ptr curr_section(new dram_section(i));
    dsections.push_back(curr_section);
  }
}

ripq_shield::~ripq_shield()
{
}

// add new Request to dsection_id
// 将一个新请求加入到DRAM的指定段中
void ripq_shield::dram_add(const Request *r, int dsection_id)
{
  assert(uint32_t(r->size()) < dsection_size);
  // 查找对象
  auto it = dram_map.find(r->kid); // FIXME delete
  assert(it == dram_map.end());

  // 目标段
  dsection_ptr target_dsection = dsections[dsection_id];
  // 将请求加入到段中
  item_ptr new_item = target_dsection->add(r);
  // 对象迁移，保证段的大小不超过限制
  dbalance(target_dsection->id);
  // 如果最后一个段的大小超过限制，驱逐对象
  while (dsections[num_dsections - 1]->filled_bytes > dsection_size)
  {
    dram_evict();
  }
  // 更新对象映射
  dram_map[r->kid] = new_item;
}

// 将新请求加入到DRAM的指定段中
ripq_shield::item_ptr ripq_shield::dram_section::add(const Request *req)
{
  // 更新缓存字节数和缓存对象数
  filled_bytes += req->size();
  num_items++;
  // 创建新对象
  item_ptr new_item(new item(*req, shared_from_this()));
  // 将新对象加入到段的对象列表中
  items.push_front(new_item);
  new_item->dram_it = items.begin();
  return new_item;
}

// DRAM段间重平衡，顺序向后迁移对象
void ripq_shield::dbalance(int start)
{
  for (uint32_t i = start; i < num_dsections - 1; i++)
  {
    while (dsections[i]->filled_bytes > dsection_size)
    {
      const Request *evicted_req = dsections[i]->evict();
      assert(evicted_req);
      dram_map[evicted_req->kid] = dsections[i + 1]->add(evicted_req);
    }
  }
}

// 从DRAM段中移除对象
void ripq_shield::dram_section::remove(item_ptr curr_item)
{
  //  assert(std::find(items.begin(), items.end(), curr_item) != items.end());

  assert(filled_bytes >= (uint32_t)curr_item->req.size());
  // 更新缓存字节数和缓存对象数
  filled_bytes -= (uint32_t)curr_item->req.size();
  num_items--;
  // 从列表中删除对象
  items.erase(curr_item->dram_it);
}

// 从DRAM段中驱逐末尾对象
const Request *ripq_shield::dram_section::evict()
{
  assert(!items.empty());
  // 末尾对象
  item_ptr last_item = items.back();
  assert(filled_bytes >= (uint32_t)last_item->req.size());
  filled_bytes -= (uint32_t)last_item->req.size();
  num_items--;
  items.pop_back();
  return &last_item->req;
}

// 从DRAM缓存中驱逐对象，驱逐末尾段的末尾对象
void ripq_shield::dram_evict()
{
  const Request *curr_req = dsections[num_dsections - 1]->evict();
  stat.bytes_cached -= curr_req->size();
  dram_map.erase(curr_req->kid);
}

// Flash eviction function
// 闪存中驱逐
void ripq_shield::evict()
{
  for (uint32_t i = 0; i < num_sections; i++)
  {
    assert(sections[i]->count_filled_bytes() == (int)sections[i]->filled_bytes);
  }
  // 末尾段
  section_ptr tail_section = sections.back();
  assert(!tail_section->blocks.empty());

  // We first have to evict the block from the section, because the reallocations might evict blocks too
  // 末尾段的末尾块
  block_ptr evicted_block = tail_section->evict_block();
  assert(!evicted_block->active);
  assert(!evicted_block->is_virtual || evicted_block->num_items == 0);
  assert(stat.bytes_cached > evicted_block->filled_bytes);

  // Remove all items from their corresponding virtual blocks before reallocating them to their new sections. Otherwise we get corner cases where we evict virtual blocks with size>0
  // 遍历被驱逐块中的对象，若对象的逻辑块不等于物理块，说明对象无效或者需要迁移，则从逻辑块中移除
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
  // 再次遍历被驱逐块中对象，将有效对象驱逐，无效对象删除，需要迁移的对象迁移
  while (!evicted_block->items.empty())
  {
    if (evicted_block->is_virtual)
      break;

    // 被驱逐块的末尾对象
    ripq::item_ptr evicted_item = evicted_block->items.back();
    assert(evicted_item->physical_block == evicted_block);

    // 末尾对象的所属逻辑块
    block_ptr virtual_block = evicted_item->virtual_block;
    assert(!evicted_block->active);

    // 将末尾对象从被驱逐块对象列表中移除
    // First pop the item out of the block
    evicted_block->items.pop_back();

    // Item can become a ghost if the key was updated with a new a value
    // 若为不为无效对象，则删除索引，无效对象则不需要进行操作
    if (!evicted_item->is_ghost)
      map.erase(evicted_item->req.kid);

    // Check if we should reallocate in another section
    // 若对象的逻辑块不等于物理块，说明对象需要迁移
    if (virtual_block != evicted_item->physical_block)
    { // Reallocate in flash
      assert(!evicted_item->physical_block->is_virtual);
      assert(!evicted_item->is_ghost);
      // 将当前对象更新到其当前逻辑块所属的段，即添加到该段的物理块中
      add(&evicted_item->req, virtual_block->s->id);

      for (uint32_t i = 0; i < num_sections; i++)
      {
        assert(sections[i]->count_filled_bytes() == (int)sections[i]->filled_bytes);
      }
    }
    else if (!evicted_item->is_ghost)
    { // 若对象的逻辑块等于物理块，且不为无效对象，本来是直接驱逐，这里重新加入到DRAM缓存的第一个段中
      // Reallocate to dram klru head
      dram_add(&evicted_item->req, 0);
    }
  }

  balance();
}

// 处理请求
size_t ripq_shield::proc(const Request *r, bool warmup)
{

  assert(r->size() > 0);

  this->warmup = warmup;
  if (!warmup)
    ++stat.accesses;

  item_ptr item_it = NULL;

  // dram中查找
  std::unordered_map<uint32_t, item_ptr>::iterator it_dram = dram_map.find(r->kid);
  if (it_dram != dram_map.end())
  {
    item_it = std::static_pointer_cast<item>(it_dram->second);
    assert(item_it->in_dram);
  }
  // flash中查找
  auto it_flash = map.find(r->kid);
  if (it_flash != map.end())
  {
    // dram和flash中只会存在一个
    assert(it_dram == dram_map.end());
    item_it = std::static_pointer_cast<item>(it_flash->second);
    assert(!item_it->in_dram);
  }

  if (item_it)
  {
    if (item_it->req.size() == r->size()) // 未更新大小
    {
      // HIT

      if (!warmup)
        ++stat.hits;

      // 若在dram中命中
      if (item_it->in_dram)
      {
        // HIT in DRAM
        // 从原段中删除
        item_it->ds->remove(item_it);
        dram_map.erase(item_it->req.kid);
        // 若在dram的第一个段中，说明为读取密集，写入到flash中
        if (item_it->ds->id == 0)
        {
          add(r, num_sections - 1); // allocate to flash
        }
        else // 否则提升到DRAM中更高的段
        {
          dram_add(r, item_it->ds->id - 1); // allocate to higer queue in dram
        }
        if (!warmup)
          ++stat.hits_dram;
      }
      else // 在flash中命中
      {
        // HIT in FLASH
        // update virtual block
        // 对象所属逻辑块所在的段
        section_ptr s = item_it->virtual_block->s;
        int new_section_id = (s->id > 0) ? (s->id - 1) : s->id;
        // 从原逻辑块删除
        item_it->virtual_block->remove(item_it);
        // 若其所在的物理块为开放块，则直接从原物理块中删除
        if (item_it->physical_block->active)
        {
          assert(item_it->physical_block == item_it->virtual_block);
          block_ptr active_block = item_it->physical_block;
          item_list::iterator iter = std::find(active_block->items.begin(), active_block->items.end(), item_it);
          assert(iter != active_block->items.end());
          // 从原开放物理块中删除
          active_block->items.erase(iter);
          map.erase(r->kid);
          // 添加到新段
          add(r, new_section_id);
        }
        else // 若其所在的物理块为关闭块，则只更新对象的逻辑块
        {
          add_virtual(item_it, new_section_id);
        }

        if (!warmup)
          ++stat.hits_flash;
        assert(item_it->virtual_block->s->id <= item_it->physical_block->s->id);
      }
      return 1; // 返回命中
    }
    else // 对象大小有变化，更新对象
    {
      // UPDATE
      if (item_it->in_dram) // 若在dram中，直接删除
      {
        item_it->ds->remove(item_it);
        dram_map.erase(item_it->req.kid);
      }
      else // 若在flash中，标记为无效
      {
        map.erase(item_it->req.kid);
        item_it->virtual_block->remove(item_it);
        item_it->virtual_block = item_it->physical_block;
        item_it->is_ghost = true;
        assert(item_it->virtual_block->s->id <= item_it->physical_block->s->id);
      }
      assert(stat.bytes_cached > (uint32_t)item_it->req.size());
      // 更新缓存字节数
      stat.bytes_cached -= (uint32_t)item_it->req.size();
    }
  }
  else
  {
    // MISS
  }
  // 更新对象的重插入，或miss的新对象的插入
  stat.bytes_cached += (uint32_t)r->size();
  // 插入到dram的末尾段中
  dram_add(r, num_dsections - 1);

  return PROC_MISS;
}
