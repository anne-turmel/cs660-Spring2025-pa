#include <db/IndexPage.hpp>
#include <stdexcept>
#include <cstring>

using namespace db;

IndexPage::IndexPage(Page &page) {
    // TODO pa2
    header = (IndexPageHeader *)page.data();
    capacity = (DEFAULT_PAGE_SIZE - sizeof(IndexPageHeader) - sizeof(size_t)) / (sizeof(size_t) + sizeof(int));
    children = reinterpret_cast<size_t *>(page.data() + DEFAULT_PAGE_SIZE - (capacity + 1) * sizeof(size_t));
    keys = reinterpret_cast<int *>(children - (capacity * sizeof(int)));
}

bool IndexPage::insert(int key, size_t child) {
    // TODO pa2
    int index = 0;
    while (index < header->size && keys[index] < key) {
        index++;
    }
    if (index < header->size && keys[index] > key) { // move subsequent keys to make room for this one
        int num_to_move = header->size - index + 1;
        std::memmove(&keys[index + 1], &keys[index], num_to_move * sizeof(int));
        std::memmove(&children[index + 1], &children[index], (num_to_move + 1) * sizeof(int));
    }
    keys[index] = key;
    children[index + 1] = child;
    header->size++;
    if (header->size >= capacity) {
        return true;
    }
    return false;
}

int IndexPage::split(IndexPage &new_page) {
    // TODO pa2
    size_t first_half = header->size / 2;
    size_t second_half = header->size - first_half;
    new_page.header->index_children = header->index_children;
    int *second_half_keys = keys + first_half;
    unsigned long *second_half_children = children + first_half;
    int new_key = second_half_keys[0];
    second_half_keys++;
    std::memmove(new_page.keys, second_half_keys, second_half * sizeof(int));
    std::memmove(new_page.children, second_half_children, second_half + 1);
    new_page.header->size = second_half - 1;
    header->size = first_half;
    return new_key;
}
