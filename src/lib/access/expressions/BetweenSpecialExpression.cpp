// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_ACCESS_BETWEENSPECIALEXPRESSION_HPP_
#define SRC_LIB_ACCESS_BETWEENSPECIALEXPRESSION_HPP_

#include "json.h"

#include "helper/make_unique.h"
#include "helper/types.h"

#include "access/expressions/AbstractExpression.h"
#include "access/expressions/ExpressionRegistration.h"

#include "storage/AbstractTable.h"
#include "storage/FixedLengthVector.h"
#include "storage/OrderPreservingDictionary.h"
#include "storage/storage_types.h"

namespace hyrise { namespace access {

template <typename T>
class BetweenSpecialExpression : public AbstractExpression {
  storage::c_atable_ptr_t _table;
  std::shared_ptr<FixedLengthVector<value_id_t>> _vector;
  std::shared_ptr<OrderPreservingDictionary<T>> _dict;
  const size_t _column;
  const T _fromValue;
  const T _toValue;
  value_id_t _fromValueid;
  value_id_t _toValueid;
 public:

  explicit BetweenSpecialExpression(
    const size_t& column, 
    const T& fromValue, 
    const T& toValue) : 
      _column(column),
      _fromValue(fromValue),
      _toValue(toValue) {};

  inline bool operator()(const size_t& row) { return _fromValueid <= _vector->getRef(_column, row) && _vector->getRef(_column, row) <= _toValueid; }

  virtual pos_list_t* match(const size_t start, const size_t stop) {
    // if from-to-range is out of bounds for dictionary
    if (_fromValue > _dict->getGreatestValue() || _toValue < _dict->getSmallestValue()) { 
      return new pos_list_t();
    } // select everything
    else if (_fromValue < _dict->getSmallestValue() && _toValue > _dict->getGreatestValue()) {
      pos_list_t* l = new pos_list_t(stop-start);
      // fill with all positions in [start, stop)
      std::iota(l->begin(), l->end(), start);
      return l;
    }

    // find an inclusive range
    // if border values do not exist, chose the next "more inner" ones
    _fromValueid = _dict->getValueIdForValueGreaterOrEqualTo(_fromValue);
    _toValueid = _dict->getValueIdForValueSmallerOrEqualTo(_toValue);

    auto pl = new pos_list_t;
    for(size_t row=start; row < stop; ++row) {
      if (this->BetweenSpecialExpression::operator()(row)) {
        pl->push_back(row);
      }
    }
    return pl;
  }


  virtual void walk(const std::vector<hyrise::storage::c_atable_ptr_t> &tables) {
    _table = tables.at(0);
    const auto& avs = _table->getAttributeVectors(_column);
    _vector = std::dynamic_pointer_cast<FixedLengthVector<value_id_t>>(avs.at(0).attribute_vector);
    _dict = std::dynamic_pointer_cast<OrderPreservingDictionary<T>>(_table->dictionaryAt(_column));
    if (!(_vector && _dict)) throw std::runtime_error("Could not extract proper structures");
  }

  virtual BetweenSpecialExpression<T> * clone() {
    return new BetweenSpecialExpression<T>(_column, _fromValue, _toValue);
  }

  static std::unique_ptr<BetweenSpecialExpression<T>> parse(const Json::Value& data);

  virtual uint determineDynamicCount(size_t max_task_size, size_t input_table_size) {
    //find closest table size in lookup

    int difference, size_index;
    size_index = 0;
    difference = abs(input_table_size - table_size[0]);

    // TODO constant values
    for(int i = 0; i < table_size_size; i++) {
      if (difference > abs(input_table_size - table_size[i])) {
        difference = abs(input_table_size - table_size[i]);
        size_index = i;
      }
    }

    int lookup_index;
    lookup_index = 0;
    difference = abs(max_task_size - lookup[size_index][0][0]);

    // TODO constant values
    // TOOD maybe sort the values the other way around.
    for(int i = 0; i < lookup_size; i++) {
      if (difference > abs(max_task_size - lookup[size_index][i][0])) {
        difference = abs(max_task_size - lookup[size_index][i][0]);
        lookup_index = i;
      }
    }
    std::cout << "TableScan determineDynamicCount: " << lookup[size_index][lookup_index][1] << std::endl;
    return lookup[size_index][lookup_index][1];
  }


protected:
  int table_size_size = 7;
  int lookup_size = 121;

int table_size[7] = {100000000, 50000000, 10000000, 5000000, 1000000, 500000, 100000};

int lookup[7][121][2] = {{{14, 1024}, {49, 992}, {15, 960}, {55, 896}, {56, 832}, {14, 800}, {15, 768}, {12, 928}, {13, 864}, {67, 704}, {19, 736}, {66, 640}, {76, 544}, {73, 576}, {23, 608}, {82, 480}, {17, 672}, {81, 464}, {94, 432}, {75, 496}, {91, 400}, {81, 448}, {88, 384}, {20, 512}, {96, 368}, {93, 416}, {90, 336}, {80, 352}, {98, 304}, {125, 193}, {103, 272}, {105, 256}, {110, 249}, {122, 177}, {118, 217}, {96, 320}, {99, 288}, {115, 225}, {115, 241}, {127, 169}, {139, 145}, {110, 233}, {145, 137}, {116, 209}, {135, 153}, {112, 201}, {157, 129}, {125, 185}, {149, 121}, {159, 113}, {142, 161}, {169, 97}, {168, 105}, {202, 73}, {261, 55}, {263, 48}, {223, 81}, {263, 54}, {202, 89}, {254, 57}, {240, 65}, {255, 61}, {251, 49}, {286, 56}, {277, 53}, {279, 50}, {283, 58}, {310, 59}, {274, 62}, {306, 63}, {297, 60}, {299, 52}, {278, 51}, {297, 64}, {654, 12}, {665, 24}, {678, 13}, {678, 14}, {731, 15}, {703, 25}, {759, 26}, {672, 47}, {685, 29}, {696, 27}, {678, 46}, {767, 16}, {674, 36}, {694, 43}, {747, 38}, {770, 37}, {762, 28}, {727, 44}, {781, 35}, {718, 45}, {717, 33}, {693, 34}, {737, 39}, {817, 17}, {739, 41}, {754, 6}, {750, 30}, {638, 40}, {849, 19}, {838, 7}, {844, 18}, {828, 31}, {715, 32}, {848, 9}, {655, 42}, {849, 8}, {863, 20}, {884, 10}, {909, 21}, {931, 22}, {906, 23}, {958, 11}, {1161, 5}, {1152, 4}, {1101, 3}, {1727, 2}, {3116, 1}},
{{6, 832}, {22, 544}, {10, 736}, {5, 928}, {7, 800}, {13, 768}, {10, 576}, {13, 704}, {9, 640}, {26, 464}, {10, 496}, {30, 384}, {9, 512}, {47, 400}, {6, 896}, {30, 416}, {9, 480}, {9, 608}, {11, 448}, {9, 672}, {5, 992}, {6, 864}, {11, 432}, {6, 960}, {5, 1024}, {17, 352}, {46, 304}, {41, 320}, {14, 368}, {46, 272}, {16, 336}, {45, 288}, {49, 249}, {47, 256}, {51, 225}, {48, 241}, {53, 233}, {49, 217}, {55, 193}, {64, 201}, {53, 209}, {65, 185}, {66, 177}, {69, 169}, {63, 153}, {63, 161}, {79, 113}, {69, 145}, {67, 137}, {70, 121}, {80, 105}, {75, 129}, {74, 97}, {104, 73}, {106, 64}, {111, 59}, {107, 60}, {106, 63}, {105, 62}, {102, 89}, {104, 81}, {104, 65}, {112, 56}, {115, 57}, {115, 54}, {115, 55}, {104, 61}, {119, 48}, {112, 58}, {113, 52}, {110, 53}, {116, 51}, {122, 49}, {113, 50}, {144, 28}, {148, 24}, {152, 31}, {139, 26}, {134, 25}, {139, 27}, {158, 30}, {158, 29}, {157, 32}, {169, 34}, {178, 33}, {172, 47}, {168, 46}, {190, 35}, {189, 44}, {178, 37}, {190, 45}, {190, 39}, {191, 43}, {185, 42}, {192, 40}, {192, 36}, {187, 38}, {190, 41}, {302, 13}, {309, 12}, {314, 15}, {311, 14}, {330, 18}, {335, 16}, {346, 23}, {345, 19}, {343, 17}, {355, 21}, {360, 20}, {375, 22}, {395, 6}, {401, 7}, {407, 10}, {399, 8}, {421, 11}, {409, 9}, {554, 5}, {545, 4}, {553, 3}, {876, 2}, {1541, 1}},
{{6, 105}, {6, 89}, {7, 121}, {10, 113}, {14, 97}, {9, 129}, {10, 137}, {7, 153}, {6, 185}, {7, 161}, {12, 81}, {11, 145}, {8, 201}, {9, 169}, {16, 73}, {8, 217}, {15, 64}, {7, 225}, {15, 65}, {16, 62}, {9, 177}, {21, 59}, {18, 56}, {18, 57}, {15, 60}, {17, 63}, {17, 54}, {18, 51}, {17, 61}, {21, 55}, {20, 47}, {20, 49}, {19, 50}, {8, 193}, {18, 58}, {7, 241}, {21, 48}, {19, 43}, {8, 209}, {19, 46}, {20, 53}, {6, 320}, {21, 42}, {18, 52}, {21, 41}, {22, 44}, {20, 40}, {6, 336}, {7, 249}, {4, 384}, {21, 45}, {7, 288}, {8, 233}, {23, 39}, {5, 368}, {8, 256}, {7, 272}, {7, 304}, {3, 400}, {5, 416}, {5, 352}, {5, 432}, {28, 30}, {28, 27}, {29, 29}, {28, 26}, {31, 32}, {5, 448}, {6, 480}, {29, 24}, {29, 28}, {30, 31}, {31, 37}, {34, 35}, {32, 34}, {33, 38}, {4, 544}, {29, 25}, {5, 496}, {31, 21}, {5, 512}, {5, 464}, {31, 22}, {32, 36}, {34, 23}, {29, 33}, {5, 576}, {30, 20}, {3, 640}, {4, 736}, {4, 768}, {4, 672}, {3, 704}, {42, 19}, {43, 18}, {4, 608}, {45, 17}, {4, 800}, {4, 832}, {45, 16}, {49, 14}, {49, 15}, {49, 13}, {3, 928}, {49, 11}, {3, 864}, {54, 12}, {46, 10}, {4, 896}, {3, 960}, {4, 992}, {3, 1024}, {64, 9}, {68, 7}, {66, 8}, {74, 6}, {81, 5}, {118, 4}, {114, 3}, {200, 2}, {361, 1}},
{{10, 60}, {9, 48}, {8, 65}, {9, 47}, {11, 51}, {6, 81}, {11, 50}, {10, 73}, {10, 55}, {10, 49}, {10, 63}, {10, 59}, {9, 45}, {10, 42}, {11, 56}, {9, 53}, {11, 61}, {10, 46}, {10, 54}, {11, 57}, {10, 64}, {11, 62}, {10, 43}, {9, 52}, {11, 58}, {10, 39}, {10, 40}, {10, 41}, {6, 89}, {6, 121}, {12, 44}, {8, 97}, {7, 105}, {15, 37}, {3, 161}, {15, 28}, {9, 113}, {14, 24}, {15, 36}, {3, 185}, {5, 153}, {14, 26}, {15, 29}, {16, 32}, {16, 35}, {14, 23}, {14, 25}, {3, 177}, {15, 38}, {15, 21}, {14, 27}, {4, 169}, {16, 30}, {15, 22}, {3, 209}, {3, 201}, {15, 20}, {3, 193}, {7, 129}, {16, 34}, {17, 31}, {18, 33}, {8, 145}, {8, 137}, {3, 217}, {3, 225}, {3, 233}, {21, 18}, {3, 241}, {2, 320}, {4, 249}, {23, 17}, {22, 19}, {2, 304}, {22, 16}, {2, 256}, {23, 15}, {23, 14}, {1, 352}, {22, 10}, {25, 11}, {2, 288}, {2, 272}, {26, 12}, {3, 336}, {2, 368}, {26, 13}, {1, 400}, {30, 9}, {1, 432}, {2, 464}, {2, 512}, {2, 496}, {1, 384}, {33, 7}, {30, 8}, {2, 544}, {35, 6}, {2, 416}, {1, 480}, {1, 448}, {1, 576}, {2, 640}, {1, 608}, {42, 5}, {2, 704}, {1, 672}, {1, 768}, {2, 864}, {1, 736}, {4, 832}, {3, 800}, {58, 3}, {3, 896}, {3, 928}, {2, 960}, {70, 4}, {4, 992}, {2, 1024}, {98, 2}, {180, 1}},
{{5, 9}, {4, 17}, {5, 10}, {5, 8}, {4, 16}, {5, 29}, {4, 19}, {5, 11}, {5, 13}, {4, 21}, {4, 18}, {3, 35}, {3, 33}, {5, 12}, {2, 31}, {5, 14}, {3, 36}, {3, 37}, {3, 40}, {6, 15}, {5, 24}, {4, 23}, {3, 32}, {4, 22}, {3, 34}, {4, 28}, {4, 25}, {2, 46}, {7, 7}, {5, 30}, {2, 49}, {3, 38}, {4, 20}, {4, 26}, {2, 50}, {3, 42}, {3, 51}, {3, 39}, {5, 27}, {3, 44}, {2, 43}, {2, 54}, {7, 6}, {3, 48}, {3, 41}, {2, 45}, {2, 52}, {2, 47}, {2, 53}, {3, 55}, {1, 58}, {2, 56}, {2, 61}, {1, 65}, {8, 5}, {1, 59}, {1, 62}, {2, 57}, {10, 4}, {1, 64}, {1, 60}, {1, 63}, {1, 73}, {14, 3}, {1, 89}, {1, 81}, {1, 97}, {1, 105}, {16, 2}, {1, 129}, {27, 1}, {1, 137}, {1, 113}, {1, 161}, {1, 121}, {1, 169}, {1, 145}, {1, 153}, {1, 241}, {1, 209}, {1, 177}, {1, 201}, {1, 272}, {1, 185}, {1, 217}, {1, 249}, {1, 336}, {1, 193}, {1, 233}, {1, 256}, {1, 416}, {1, 225}, {1, 448}, {1, 352}, {1, 432}, {1, 464}, {1, 320}, {1, 480}, {1, 384}, {1, 400}, {1, 512}, {1, 288}, {1, 640}, {1, 608}, {1, 544}, {1, 576}, {1, 368}, {2, 704}, {0, 304}, {1, 496}, {2, 800}, {1, 736}, {2, 768}, {3, 832}, {2, 896}, {1, 672}, {4, 928}, {4, 992}, {1, 864}, {4, 960}, {4, 1024}},
{{2, 10}, {2, 9}, {2, 8}, {1, 21}, {1, 16}, {2, 11}, {1, 17}, {1, 18}, {3, 7}, {3, 13}, {1, 19}, {3, 6}, {2, 12}, {2, 15}, {1, 20}, {2, 14}, {2, 26}, {1, 31}, {1, 25}, {2, 23}, {1, 22}, {4, 5}, {2, 24}, {4, 4}, {1, 27}, {1, 28}, {1, 29}, {1, 30}, {1, 32}, {1, 33}, {1, 34}, {1, 39}, {1, 37}, {1, 35}, {1, 38}, {1, 36}, {1, 41}, {1, 45}, {1, 42}, {1, 46}, {7, 3}, {1, 49}, {1, 43}, {13, 1}, {1, 40}, {7, 2}, {1, 44}, {1, 47}, {1, 48}, {1, 53}, {1, 51}, {1, 50}, {1, 54}, {1, 52}, {1, 59}, {1, 55}, {1, 58}, {0, 56}, {1, 61}, {1, 63}, {0, 57}, {1, 60}, {0, 62}, {0, 64}, {0, 65}, {1, 73}, {1, 97}, {0, 89}, {1, 129}, {1, 81}, {1, 105}, {1, 145}, {1, 193}, {0, 113}, {1, 137}, {1, 169}, {1, 121}, {1, 233}, {1, 153}, {1, 185}, {0, 177}, {1, 161}, {1, 249}, {1, 217}, {1, 225}, {1, 201}, {1, 209}, {1, 241}, {1, 320}, {1, 272}, {1, 384}, {1, 288}, {1, 464}, {1, 304}, {1, 336}, {1, 416}, {1, 480}, {0, 256}, {1, 368}, {1, 432}, {1, 400}, {1, 496}, {1, 544}, {1, 448}, {1, 608}, {1, 576}, {1, 352}, {1, 512}, {1, 672}, {3, 704}, {3, 736}, {1, 640}, {1, 768}, {2, 800}, {3, 832}, {2, 928}, {3, 960}, {2, 896}, {2, 864}, {4, 992}, {2, 1024}},
{{1, 5}, {1, 6}, {0, 8}, {2, 1}, {1, 2}, {0, 7}, {0, 10}, {1, 3}, {1, 4}, {0, 12}, {0, 9}, {0, 13}, {0, 11}, {0, 14}, {0, 19}, {0, 15}, {0, 17}, {0, 16}, {0, 18}, {0, 20}, {0, 21}, {0, 23}, {0, 22}, {0, 24}, {0, 26}, {0, 25}, {0, 27}, {0, 28}, {0, 29}, {0, 34}, {0, 30}, {0, 31}, {0, 32}, {0, 38}, {0, 35}, {0, 33}, {0, 36}, {1, 41}, {0, 40}, {0, 37}, {0, 46}, {0, 42}, {0, 39}, {0, 43}, {0, 44}, {0, 45}, {0, 50}, {0, 48}, {0, 49}, {0, 53}, {0, 47}, {0, 54}, {0, 56}, {0, 59}, {0, 52}, {0, 64}, {0, 51}, {0, 55}, {0, 58}, {0, 63}, {0, 57}, {0, 62}, {0, 61}, {0, 60}, {0, 65}, {0, 81}, {0, 73}, {1, 121}, {0, 89}, {0, 97}, {0, 105}, {0, 113}, {1, 177}, {1, 169}, {1, 145}, {0, 137}, {1, 193}, {1, 161}, {0, 129}, {0, 153}, {1, 201}, {1, 225}, {1, 233}, {1, 272}, {1, 249}, {0, 185}, {1, 217}, {1, 241}, {1, 256}, {1, 336}, {0, 209}, {1, 352}, {1, 400}, {1, 368}, {1, 416}, {1, 320}, {1, 304}, {1, 288}, {1, 384}, {1, 432}, {1, 464}, {1, 448}, {1, 512}, {1, 576}, {1, 480}, {3, 704}, {1, 496}, {1, 672}, {1, 544}, {1, 608}, {1, 640}, {4, 864}, {2, 800}, {3, 768}, {4, 832}, {2, 736}, {2, 928}, {2, 960}, {3, 992}, {3, 1024}, {1, 896}}};

};

template<> std::unique_ptr<BetweenSpecialExpression<hyrise_int_t>> BetweenSpecialExpression<hyrise_int_t>::parse(const Json::Value& data) {
      return make_unique<BetweenSpecialExpression<hyrise_int_t>>(
      data["column"].asUInt(),
      data["fromValue"].asInt(),
      data["toValue"].asInt());
};

template<> std::unique_ptr<BetweenSpecialExpression<hyrise_float_t>> BetweenSpecialExpression<hyrise_float_t>::parse(const Json::Value& data) {
      return make_unique<BetweenSpecialExpression<hyrise_float_t>>(
      data["column"].asUInt(),
      data["fromValue"].asDouble(),
      data["toValue"].asDouble());
};

template<> std::unique_ptr<BetweenSpecialExpression<hyrise_string_t>> BetweenSpecialExpression<hyrise_string_t>::parse(const Json::Value& data) {
      return make_unique<BetweenSpecialExpression<hyrise_string_t>>(
      data["column"].asUInt(),
      data["fromValue"].asString(),
      data["toValue"].asString());
};

namespace {
  auto _1 = Expressions::add<BetweenSpecialExpression<hyrise_int_t>>("hyrise::intbetween");
  auto _2 = Expressions::add<BetweenSpecialExpression<hyrise_float_t>>("hyrise::floatbetween");
  auto _3 = Expressions::add<BetweenSpecialExpression<hyrise_string_t>>("hyrise::stringbetween");
}


}}

#endif
