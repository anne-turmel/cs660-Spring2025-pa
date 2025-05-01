#include <stdexcept>
#include <db/ColumnStats.hpp>

using namespace db;

ColumnStats::ColumnStats(unsigned buckets, int min, int max)
// TODO pa4: some code goes here
: bucket_quantities(std::min(buckets, static_cast<unsigned>(max - min)))
{
    // TODO pa4: some code goes here
    this->max = max;
    this->min = min;
    total = 0;
}

void ColumnStats::addValue(int v) {
    // TODO pa4: some code goes here
    int bucket = (v - min) / ((max + 1 - min) / bucket_quantities.size());
    bucket_quantities[bucket]++;
    total++;
}

size_t ColumnStats::estimateCardinality(PredicateOp op, int v) const {
    // TODO pa4: some code goes here
    size_t result = 0;
    int width = (max + 1 - min) / bucket_quantities.size();
    if (v > max) {
        v = max;
    }
    if (v < min) {
        v = min;
    }
    int bucket = (v - min) / width;
    switch (op) {
        case PredicateOp::EQ:
            result = bucket_quantities[bucket] / width;
            break;
        case PredicateOp::NE:
            result = total - (bucket_quantities[bucket] / width);
            break;
        case PredicateOp::GT: {
            int right_endpoint = min + ((bucket + 1) * width);
            result = bucket_quantities[bucket] * (right_endpoint - v - 1) / width;
            for (int i = bucket + 1; i < bucket_quantities.size(); i++) {
                result += bucket_quantities[i];
            }}
            break;
        case PredicateOp::GE: {
            int right_endpoint = min + ((bucket + 1) * width);
            result = bucket_quantities[bucket] * (right_endpoint - v) / width;
            for (int i = bucket + 1; i < bucket_quantities.size(); i++) {
                result += bucket_quantities[i];
            }
        }
            break;
        case PredicateOp::LE:
           {int left_endpoint = min + (bucket * width);
            result = bucket_quantities[bucket] * (v - left_endpoint + 1) / width;
            for (int i = bucket - 1; i >= 0; i--) {
                result += bucket_quantities[i];
            }
        }
            break;
        case PredicateOp::LT: {
            int left_endpoint = min + (bucket * width);
            result = bucket_quantities[bucket] * (v - left_endpoint) / width;
            for (int i = bucket - 1; i >= 0; i--) {
                result += bucket_quantities[i];
            }
        }
            break;
        default:
            throw std::invalid_argument("Invalid PredicateOp");
    }
    return result;
}
