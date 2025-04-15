#include <map>
#include <stdexcept>
#include <bits/ios_base.h>
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
    // Keep track of groups corresponding to aggregates
    std::unordered_map<field_t, int> groups;
    // Keep track of number of members of each group
    std::unordered_map<field_t, int> group_sizes;
    // If there are no groups, then aggregate all data for one big group
    if (!agg.group.has_value()) {
        group_sizes["all"] = 0;
    }
    // iterate through all tuples to generate aggregate values
    auto it_in = in.begin();
    while (it_in != in.end()) {
        auto it = *it_in;
        int target = std::get<int>(it.get_field(in.getTupleDesc().index_of(agg.field)));
        if (agg.group.has_value()) {
            field_t group = it.get_field(in.getTupleDesc().index_of(agg.group.value()));
            if (groups.find(group) == groups.end()) {
                if (agg.op == AggregateOp::COUNT) {
                    groups[group] = 1;
                } else {
                    groups[group] = target;
                }
                if (agg.op == AggregateOp::AVG) {
                    group_sizes[group] = 1;
                }
            } else {
                if (agg.op == AggregateOp::AVG) {
                    groups[group] += target;
                    group_sizes[group]++;
                }
                if (agg.op == AggregateOp::MIN) {
                    if (groups[group] > target) {
                        groups[group] = target;
                    }
                }
                if (agg.op == AggregateOp::MAX) {
                    if (groups[group] < target) {
                        groups[group] = target;
                    }
                }
                if (agg.op == AggregateOp::SUM) {
                    groups[group] += target;
                }
                if (agg.op == AggregateOp::COUNT) {
                    groups[group]++;
                }
            }
        }
        else {
            // If the aggregate operation is average, just add the field value for now
            // Taking the average will occur after all values are added
            if (agg.op == AggregateOp::AVG) {
                groups["all"] += target;
                group_sizes["all"] += 1;
            }
            if (agg.op == AggregateOp::MIN) {
                if (groups["all"] > target) {
                    groups["all"] = target;
                }
            }
            if (agg.op == AggregateOp::MAX) {
                if (groups["all"] < target) {
                    groups["all"] = target;
                }
            }
            if (agg.op == AggregateOp::SUM) {
                groups["all"] += target;
            }
            if (agg.op == AggregateOp::COUNT) {
                groups["all"] += 1;
            }
        }
        ++it_in;
    }
    auto group_it = groups.begin();
    while (group_it != groups.end()) {
        if (agg.group.has_value()) {
            if (agg.op == AggregateOp::AVG) {
                out.insertTuple(std::vector<field_t>{group_it->first, (double)group_it->second/(double)group_sizes.at(group_it->first)});
            } else {
                out.insertTuple(std::vector<field_t>{group_it->first, group_it->second});
            }
        } else {
            if (agg.op == AggregateOp::AVG) {
                out.insertTuple(std::vector<field_t>{(double)group_it->second / (double)group_sizes.at(group_it->first)});
            } else {
                out.insertTuple(std::vector<field_t>{group_it->second});
            }
        }
        ++group_it;
    }
}

void db::join(const DbFile &left, const DbFile &right, DbFile &out, const JoinPredicate &pred) {
    // TODO: Implement this function
    auto left_it = left.begin();
    while (left_it != left.end()) {
        auto right_it = right.begin();
        while (right_it != right.end()) {
            auto left_tuple = *left_it;
            auto right_tuple = *right_it;
            field_t left_field = left_tuple.get_field(left.getTupleDesc().index_of(pred.left));
            field_t right_field = right_tuple.get_field(right.getTupleDesc().index_of(pred.right));
            bool tuple_matches = true;
            switch (pred.op) {
                case PredicateOp::EQ:
                    if (left_field != right_field) {
                        tuple_matches = false;
                    }
                break;
                case PredicateOp::NE:
                    if (left_field == right_field) {
                        tuple_matches = false;
                    }
                break;
                case PredicateOp::GT:
                    if (left_field <= right_field) {
                        tuple_matches = false;
                    }
                break;
                case PredicateOp::LT:
                    if (left_field >= right_field) {
                        tuple_matches = false;
                    }
                break;
                case PredicateOp::GE:
                    if (left_field < right_field) {
                        tuple_matches = false;
                    }
                break;
                case PredicateOp::LE:
                    if (left_field <= right_field) {
                        tuple_matches = false;
                    }
                break;
                default:
                    throw std::invalid_argument("Invalid operation");
            }
            if (tuple_matches) {
                std::vector<field_t> out_fields;
                for (int i = 0; i < left_tuple.size(); i++) {
                    out_fields.push_back(left_tuple.get_field(i));
                }
                for (int i = 0; i < right_tuple.size(); i++) {
                    if (i != right.getTupleDesc().index_of(pred.right) || pred.op != PredicateOp::EQ) {
                        out_fields.push_back(right_tuple.get_field(i));
                    }
                }
                out.insertTuple(out_fields);
            }
            ++right_it;
        }
        ++left_it;
    }
}
