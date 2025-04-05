#include <cmath>
#include <cstring>
#include <memory_resource>
#include <db/BTreeFile.hpp>
#include <db/Database.hpp>
#include <db/IndexPage.hpp>
#include <db/LeafPage.hpp>
#include <stdexcept>

using namespace db;

BTreeFile::BTreeFile(const std::string &name, const TupleDesc &td, size_t key_index)
        : DbFile(name, td), key_index(key_index) {}

void BTreeFile::insertTuple(const Tuple &t) {
    // TODO pa2
    if (!td.compatible(t)) {
        throw std::runtime_error("Tuple not compatible with TupleDesc");
    }

    // get root
    BufferPool &bufferPool = getDatabase().getBufferPool();
    PageId pid{name, 0};
    Page &p = bufferPool.getPage(pid);

    // keep track of pages seen so splitting can occur as necessary
    IndexPage* page_path[(int)std::log(numPages)];
    size_t page_nums[(int)std::log(numPages)];
    int num_pages_seen = 0;
    IndexPage ip(p);
    page_path[num_pages_seen] = &ip;
    page_nums[num_pages_seen] = 0;
    if (ip.header->size != 0) {
        int search_key = std::get<int>(t.get_field(key_index));
        int search_guide;
        do {
            for (search_guide = 0; search_guide < page_path[num_pages_seen]->header->size; search_guide++) {
                if (page_path[num_pages_seen]->keys[search_guide] <= search_key) {
                    break;
                }
            }
            if (page_path[num_pages_seen]->header->index_children) {
                num_pages_seen++;
                page_nums[num_pages_seen] = page_path[num_pages_seen - 1]->children[search_guide + 1];
                IndexPage child(bufferPool.getPage(PageId{name, page_nums[num_pages_seen]}));
                page_path[num_pages_seen] = &child;
            } else {
                break;
            }
        } while (true);
        LeafPage child(bufferPool.getPage(PageId{name, page_path[num_pages_seen]->children[search_guide + 1]}), td, key_index);
        if (child.insertTuple(t)) {
            db::Page new_leaf_page{};
            db::LeafPage new_leaf{new_leaf_page, td, key_index};
            int new_key = child.split(new_leaf);
            if (page_path[num_pages_seen]->insert(new_key, page_path[num_pages_seen]->children[search_guide])) {
                while (num_pages_seen > 0) {
                    db::Page new_page{};
                    db::IndexPage new_index{new_page};
                    new_key = page_path[num_pages_seen]->split(new_index);
                    if (!page_path[num_pages_seen - 1]->insert(new_key, page_nums[num_pages_seen])) {
                        break;
                    }
                    num_pages_seen--;
                }
                if (num_pages_seen <= 0) {
                    db::Page new_root_page{};
                    db::IndexPage new_root{new_root_page};
                }
            }
        }
    }
}

void BTreeFile::deleteTuple(const Iterator &it) {
    // Do not implement
}

Tuple BTreeFile::getTuple(const Iterator &it) const {
    // TODO pa2
    BufferPool &bufferPool = getDatabase().getBufferPool();
    PageId pid{name, it.page};
    Page &p = bufferPool.getPage(pid);
    LeafPage lp(p, td, key_index);
    return lp.getTuple(it.slot);
}

void BTreeFile::next(Iterator &it) const {
    // TODO pa2
    BufferPool &bufferPool = getDatabase().getBufferPool();
    if (it.page != 0) {
        PageId pid{name, it.page};
        Page &p = bufferPool.getPage(pid);
        const LeafPage lp(p, td, key_index);
        it.slot++;
        if (it.slot <= lp.header->size) {
            return;
        }
        it.page = lp.header->next_leaf;
    }
    while (it.page != 0) {
        PageId pid{name, it.page};
        Page &p = bufferPool.getPage(pid);
        const LeafPage lp(p, td, key_index);
        it.slot = 0;
        if (it.slot <= lp.header->size) {
            return;
        }
        it.page = lp.header->next_leaf;
    }
}

Iterator BTreeFile::begin() const {
    // TODO pa2
    BufferPool &bufferPool = getDatabase().getBufferPool();
    PageId pid{name, 0};
    Page &p = bufferPool.getPage(pid);
    IndexPage ip(p);
    if (ip.header->size != 0) {
        do {
            if (ip.header->index_children) {
                IndexPage child(bufferPool.getPage(PageId{name, ip.children[0]}));
                ip = child;
            } else {
                return {*this, ip.children[0], 0};
            }
        } while (true);
    }
    return {*this, 0, 0};
}

Iterator BTreeFile::end() const {
    // TODO pa2
    BufferPool &bufferPool = getDatabase().getBufferPool();
    PageId pid{name, 0};
    Page &p = bufferPool.getPage(pid);
    IndexPage ip(p);
    if (ip.header->size != 0) {
        do {
            if (ip.header->index_children) {
                IndexPage child(bufferPool.getPage(PageId{name, ip.children[ip.header->size - 1]}));
                ip = child;
            } else {
                return {*this, ip.children[ip.header->size], 0};
            }
        } while (true);
    }
    return {*this, 0, 0};
}
