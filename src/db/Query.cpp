#include <map>
#include <stdexcept>
#include <db/Query.hpp>

using namespace db;

void db::projection(const DbFile &in, DbFile &out, const std::vector<std::string> &field_names) {
    // TODO: Implement this function
    auto it_in = in.begin();
    while (it_in != in.end()) {
        auto it = *it_in;
        std::vector<field_t> fields;
        for (const auto &field_name : field_names) {
            field_t field = it.get_field(in.getTupleDesc().index_of(field_name));
            fields.push_back(field);
        }
        out.insertTuple({fields});
        ++it_in;
    }
}

void db::filter(const DbFile &in, DbFile &out, const std::vector<FilterPredicate> &pred) {
    // TODO: Implement this function
    auto it_in = in.begin();
    while (it_in != in.end()) {
        bool tuple_matches = true;
        auto it = *it_in;
        auto pred_it = pred.begin();
        while (pred_it != pred.end()) {
            auto current_pred = *pred_it;
            field_t target = it.get_field(in.getTupleDesc().index_of(current_pred.field_name));
            switch (current_pred.op) {
                case PredicateOp::EQ:
                    if (target != current_pred.value) {
                        tuple_matches = false;
                    }
                    break;
                case PredicateOp::NE:
                    if (target == current_pred.value) {
                        tuple_matches = false;
                    }
                    break;
                case PredicateOp::LT:
                    if (target >= current_pred.value) {
                        tuple_matches = false;
                    }
                    break;
                case PredicateOp::LE:
                    if (target > current_pred.value) {
                        tuple_matches = false;
                    }
                    break;
                case PredicateOp::GT:
                    if (target <= current_pred.value) {
                        tuple_matches = false;
                    }
                    break;
                case PredicateOp::GE:
                    if (target < current_pred.value) {
                        tuple_matches = false;
                    }
                    break;
                default:
                    throw std::invalid_argument("Predicate not supported");
            }
            if (!tuple_matches) {
                break;
            }
            ++pred_it;
        }
        if (tuple_matches) {
            out.insertTuple(it);
        }
        ++it_in;
    }
}

void db::aggregate(const DbFile &in, DbFile &out, const Aggregate &agg) {
    // TODO: Implement this function
}

void db::join(const DbFile &left, const DbFile &right, DbFile &out, const JoinPredicate &pred) {
    // TODO: Implement this function
}
