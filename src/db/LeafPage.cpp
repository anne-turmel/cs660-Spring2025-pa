#include <db/LeafPage.hpp>
#include <stdexcept>
#include <bits/ranges_util.h>
#include <cstring>

using namespace db;

LeafPage::LeafPage(Page &page, const TupleDesc &td, size_t key_index) : td(td), key_index(key_index) {
    // TODO pa2
    capacity = (DEFAULT_PAGE_SIZE - sizeof(LeafPageHeader)) / td.length();
    header = (LeafPageHeader *)page.data();
    data = page.data() + DEFAULT_PAGE_SIZE - td.length() * capacity;
}

bool LeafPage::insertTuple(const Tuple &t) {
    // TODO pa2
    field_t key = t.get_field(key_index);
    size_t slot = 0;
    while (slot < header->size && getTuple(slot).get_field(key_index) < key) {
        slot++;
    }
    uint8_t *slotData = data + slot * td.length();
    if (slot < header->size && getTuple(slot).get_field(key_index) > key) {
        int mem_to_copy = td.length() * (header->size - slot);
        std::copy_backward(slotData, slotData + mem_to_copy, slotData + mem_to_copy + td.length());
    }
    if (slot >= header->size || getTuple(slot).get_field(key_index) != key) {
        header->size++;
    }
    td.serialize(slotData, t);
    if (header->size >= capacity) {
        return true;
    }
    return false;
}

int LeafPage::split(LeafPage &new_page) {
    // TODO pa2
    size_t first_half = header->size / 2;
    size_t second_half = header->size - first_half;
    new_page.header->next_leaf = header->next_leaf;
    uint8_t *second_half_data = data + first_half * td.length();
    field_t new_start = td.deserialize(second_half_data).get_field(key_index);
    std::memmove(new_page.data, second_half_data, (second_half * td.length()));
    new_page.header->size = second_half;
    header->size = first_half;
    return std::get<int>(new_start);
}

Tuple LeafPage::getTuple(size_t slot) const {
    // TODO pa2
    uint8_t *slotdata = data + slot * td.length();
    return td.deserialize(slotdata);
}
