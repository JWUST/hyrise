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

int lookup[7][121][2] = {{{4, 1024}, {5, 992}, {4, 960}, {5, 896}, {5, 832}, {5, 800}, {5, 768}, {4, 928}, {5, 864}, {8, 704}, {7, 736}, {9, 640}, {10, 544}, {10, 576}, {8, 608}, {11, 480}, {7, 672}, {12, 464}, {13, 432}, {11, 496}, {12, 400}, {11, 448}, {13, 384}, {9, 512}, {18, 368}, {12, 416}, {19, 336}, {16, 352}, {20, 304}, {27, 193}, {22, 272}, {23, 256}, {24, 249}, {36, 177}, {27, 217}, {20, 320}, {22, 288}, {26, 225}, {26, 241}, {38, 169}, {45, 145}, {25, 233}, {48, 137}, {29, 209}, {46, 153}, {25, 201}, {53, 129}, {37, 185}, {53, 121}, {53, 113}, {43, 161}, {62, 97}, {58, 105}, {109, 73}, {144, 55}, {168, 48}, {107, 81}, {149, 54}, {96, 89}, {140, 57}, {129, 65}, {134, 61}, {158, 49}, {157, 56}, {159, 53}, {170, 50}, {151, 58}, {161, 59}, {148, 62}, {153, 63}, {157, 60}, {175, 52}, {157, 51}, {149, 64}, {661, 12}, {620, 24}, {678, 13}, {655, 14}, {691, 15}, {665, 25}, {714, 26}, {417, 47}, {644, 29}, {682, 27}, {445, 46}, {704, 16}, {561, 36}, {490, 43}, {584, 38}, {622, 37}, {718, 28}, {499, 44}, {668, 35}, {483, 45}, {654, 33}, {608, 34}, {564, 39}, {755, 17}, {551, 41}, {780, 6}, {716, 30}, {494, 40}, {760, 19}, {846, 7}, {776, 18}, {787, 31}, {668, 32}, {850, 9}, {508, 42}, {856, 8}, {752, 20}, {831, 10}, {784, 21}, {800, 22}, {759, 23}, {881, 11}, {1084, 5}, {1112, 4}, {1052, 3}, {1475, 2}, {3116, 1}},
{{2, 832}, {4, 544}, {3, 736}, {2, 928}, {2, 800}, {2, 768}, {4, 576}, {3, 704}, {4, 640}, {5, 464}, {4, 496}, {5, 384}, {4, 512}, {6, 400}, {2, 896}, {6, 416}, {4, 480}, {4, 608}, {5, 448}, {3, 672}, {2, 992}, {2, 864}, {5, 432}, {2, 960}, {2, 1024}, {7, 352}, {10, 304}, {9, 320}, {7, 368}, {10, 272}, {7, 336}, {10, 288}, {11, 249}, {11, 256}, {12, 225}, {11, 241}, {12, 233}, {12, 217}, {12, 193}, {15, 201}, {13, 209}, {18, 185}, {19, 177}, {20, 169}, {20, 153}, {20, 161}, {26, 113}, {22, 145}, {20, 137}, {24, 121}, {28, 105}, {24, 129}, {29, 97}, {48, 73}, {52, 64}, {57, 59}, {56, 60}, {50, 63}, {54, 62}, {42, 89}, {44, 81}, {50, 65}, {59, 56}, {63, 57}, {65, 54}, {62, 55}, {52, 61}, {68, 48}, {61, 58}, {64, 52}, {61, 53}, {66, 51}, {72, 49}, {65, 50}, {100, 28}, {117, 24}, {112, 31}, {102, 26}, {99, 25}, {102, 27}, {115, 30}, {118, 29}, {114, 32}, {109, 34}, {130, 33}, {105, 47}, {106, 46}, {133, 35}, {118, 44}, {138, 37}, {118, 45}, {130, 39}, {125, 43}, {120, 42}, {127, 40}, {139, 36}, {137, 38}, {127, 41}, {306, 13}, {310, 12}, {296, 15}, {301, 14}, {292, 18}, {312, 16}, {278, 23}, {303, 19}, {321, 17}, {308, 21}, {308, 20}, {323, 22}, {386, 6}, {383, 7}, {366, 10}, {389, 8}, {377, 11}, {403, 9}, {485, 5}, {524, 4}, {529, 3}, {741, 2}, {1541, 1}},
{{4, 105}, {5, 89}, {4, 121}, {5, 113}, {7, 97}, {4, 129}, {4, 137}, {4, 153}, {3, 185}, {3, 161}, {6, 81}, {4, 145}, {3, 201}, {4, 169}, {8, 73}, {3, 217}, {9, 64}, {3, 225}, {9, 65}, {10, 62}, {4, 177}, {10, 59}, {11, 56}, {10, 57}, {9, 60}, {9, 63}, {10, 54}, {10, 51}, {10, 61}, {12, 55}, {12, 47}, {12, 49}, {11, 50}, {4, 193}, {10, 58}, {3, 241}, {12, 48}, {13, 43}, {4, 209}, {12, 46}, {12, 53}, {2, 320}, {14, 42}, {11, 52}, {15, 41}, {13, 44}, {14, 40}, {2, 336}, {3, 249}, {2, 384}, {13, 45}, {3, 288}, {4, 233}, {16, 39}, {2, 368}, {3, 256}, {3, 272}, {3, 304}, {2, 400}, {2, 416}, {2, 352}, {2, 432}, {20, 30}, {21, 27}, {22, 29}, {23, 26}, {22, 32}, {2, 448}, {2, 480}, {22, 24}, {23, 28}, {22, 31}, {22, 37}, {24, 35}, {23, 34}, {21, 38}, {1, 544}, {23, 25}, {2, 496}, {25, 21}, {2, 512}, {2, 464}, {25, 22}, {23, 36}, {27, 23}, {23, 33}, {1, 576}, {25, 20}, {1, 640}, {1, 736}, {1, 768}, {1, 672}, {1, 704}, {33, 19}, {35, 18}, {1, 608}, {38, 17}, {1, 800}, {1, 832}, {40, 16}, {47, 14}, {46, 15}, {50, 13}, {1, 928}, {53, 11}, {1, 864}, {55, 12}, {48, 10}, {1, 896}, {1, 960}, {1, 992}, {1, 1024}, {65, 9}, {71, 7}, {69, 8}, {77, 6}, {85, 5}, {115, 4}, {110, 3}, {168, 2}, {361, 1}},
{{5, 60}, {5, 48}, {4, 65}, {5, 47}, {6, 51}, {3, 81}, {6, 50}, {5, 73}, {6, 55}, {6, 49}, {5, 63}, {5, 59}, {6, 45}, {7, 42}, {5, 56}, {5, 53}, {6, 61}, {7, 46}, {6, 54}, {6, 57}, {5, 64}, {6, 62}, {6, 43}, {5, 52}, {6, 58}, {7, 39}, {7, 40}, {6, 41}, {4, 89}, {3, 121}, {7, 44}, {4, 97}, {3, 105}, {11, 37}, {2, 161}, {12, 28}, {4, 113}, {12, 24}, {11, 36}, {2, 185}, {2, 153}, {11, 26}, {12, 29}, {12, 32}, {12, 35}, {12, 23}, {11, 25}, {2, 177}, {10, 38}, {13, 21}, {11, 27}, {2, 169}, {13, 30}, {13, 22}, {2, 209}, {2, 201}, {12, 20}, {2, 193}, {4, 129}, {13, 34}, {14, 31}, {14, 33}, {3, 145}, {4, 137}, {2, 217}, {1, 225}, {1, 233}, {17, 18}, {1, 241}, {1, 320}, {2, 249}, {19, 17}, {18, 19}, {1, 304}, {20, 16}, {1, 256}, {22, 15}, {23, 14}, {1, 352}, {23, 10}, {26, 11}, {1, 288}, {1, 272}, {27, 12}, {1, 336}, {1, 368}, {26, 13}, {1, 400}, {29, 9}, {1, 432}, {1, 464}, {1, 512}, {1, 496}, {1, 384}, {33, 7}, {30, 8}, {1, 544}, {35, 6}, {1, 416}, {1, 480}, {1, 448}, {1, 576}, {1, 640}, {1, 608}, {40, 5}, {1, 704}, {1, 672}, {1, 768}, {1, 864}, {1, 736}, {1, 832}, {1, 800}, {55, 3}, {1, 896}, {1, 928}, {1, 960}, {66, 4}, {1, 992}, {1, 1024}, {83, 2}, {180, 1}},
{{4, 9}, {4, 17}, {4, 10}, {5, 8}, {4, 16}, {4, 29}, {3, 19}, {4, 11}, {4, 13}, {3, 21}, {3, 18}, {2, 35}, {2, 33}, {5, 12}, {2, 31}, {4, 14}, {2, 36}, {2, 37}, {2, 40}, {5, 15}, {4, 24}, {4, 23}, {2, 32}, {4, 22}, {2, 34}, {3, 28}, {4, 25}, {1, 46}, {6, 7}, {4, 30}, {2, 49}, {2, 38}, {4, 20}, {3, 26}, {1, 50}, {2, 42}, {2, 51}, {2, 39}, {4, 27}, {2, 44}, {2, 43}, {1, 54}, {7, 6}, {2, 48}, {2, 41}, {2, 45}, {1, 52}, {1, 47}, {1, 53}, {2, 55}, {1, 58}, {1, 56}, {1, 61}, {1, 65}, {8, 5}, {1, 59}, {1, 62}, {1, 57}, {10, 4}, {1, 64}, {1, 60}, {1, 63}, {1, 73}, {12, 3}, {1, 89}, {1, 81}, {1, 97}, {1, 105}, {14, 2}, {0, 129}, {27, 1}, {0, 137}, {1, 113}, {0, 161}, {0, 121}, {0, 169}, {0, 145}, {0, 153}, {1, 241}, {0, 209}, {0, 177}, {0, 201}, {0, 272}, {0, 185}, {0, 217}, {1, 249}, {0, 336}, {0, 193}, {0, 233}, {0, 256}, {1, 416}, {0, 225}, {1, 448}, {0, 352}, {1, 432}, {1, 464}, {0, 320}, {0, 480}, {0, 384}, {0, 400}, {0, 512}, {0, 288}, {1, 640}, {1, 608}, {1, 544}, {1, 576}, {0, 368}, {1, 704}, {0, 304}, {0, 496}, {1, 800}, {0, 736}, {1, 768}, {1, 832}, {1, 896}, {0, 672}, {1, 928}, {1, 992}, {1, 864}, {1, 960}, {0, 1024}},
{{2, 10}, {2, 9}, {2, 8}, {1, 21}, {1, 16}, {2, 11}, {1, 17}, {1, 18}, {3, 7}, {2, 13}, {1, 19}, {3, 6}, {2, 12}, {2, 15}, {1, 20}, {2, 14}, {1, 26}, {1, 31}, {1, 25}, {1, 23}, {1, 22}, {4, 5}, {1, 24}, {4, 4}, {1, 27}, {1, 28}, {1, 29}, {1, 30}, {1, 32}, {1, 33}, {1, 34}, {1, 39}, {1, 37}, {1, 35}, {1, 38}, {1, 36}, {1, 41}, {1, 45}, {1, 42}, {1, 46}, {6, 3}, {1, 49}, {1, 43}, {13, 1}, {1, 40}, {7, 2}, {1, 44}, {0, 47}, {0, 48}, {0, 53}, {0, 51}, {0, 50}, {0, 54}, {0, 52}, {0, 59}, {0, 55}, {0, 58}, {0, 56}, {0, 61}, {0, 63}, {0, 57}, {0, 60}, {0, 62}, {0, 64}, {0, 65}, {0, 73}, {0, 97}, {0, 89}, {0, 129}, {0, 81}, {0, 105}, {0, 145}, {0, 193}, {0, 113}, {0, 137}, {0, 169}, {0, 121}, {0, 233}, {0, 153}, {0, 185}, {0, 177}, {0, 161}, {0, 249}, {0, 217}, {0, 225}, {0, 201}, {0, 209}, {0, 241}, {0, 320}, {0, 272}, {0, 384}, {0, 288}, {0, 464}, {0, 304}, {0, 336}, {0, 416}, {1, 480}, {0, 256}, {0, 368}, {0, 432}, {0, 400}, {0, 496}, {1, 544}, {0, 448}, {1, 608}, {1, 576}, {0, 352}, {0, 512}, {1, 672}, {1, 704}, {1, 736}, {1, 640}, {0, 768}, {1, 800}, {1, 832}, {1, 928}, {1, 960}, {1, 896}, {0, 864}, {1, 992}, {1, 1024}},
{{0, 5}, {0, 6}, {0, 8}, {2, 1}, {1, 2}, {0, 7}, {0, 10}, {1, 3}, {1, 4}, {0, 12}, {0, 9}, {0, 13}, {0, 11}, {0, 14}, {0, 19}, {0, 15}, {0, 17}, {0, 16}, {0, 18}, {0, 20}, {0, 21}, {0, 23}, {0, 22}, {0, 24}, {0, 26}, {0, 25}, {0, 27}, {0, 28}, {0, 29}, {0, 34}, {0, 30}, {0, 31}, {0, 32}, {0, 38}, {0, 35}, {0, 33}, {0, 36}, {0, 41}, {0, 40}, {0, 37}, {0, 46}, {0, 42}, {0, 39}, {0, 43}, {0, 44}, {0, 45}, {0, 50}, {0, 48}, {0, 49}, {0, 53}, {0, 47}, {0, 54}, {0, 56}, {0, 59}, {0, 52}, {0, 64}, {0, 51}, {0, 55}, {0, 58}, {0, 63}, {0, 57}, {0, 62}, {0, 61}, {0, 60}, {0, 65}, {0, 81}, {0, 73}, {0, 121}, {0, 89}, {0, 97}, {0, 105}, {0, 113}, {0, 177}, {0, 169}, {0, 145}, {0, 137}, {0, 193}, {0, 161}, {0, 129}, {0, 153}, {0, 201}, {0, 225}, {0, 233}, {0, 272}, {0, 249}, {0, 185}, {0, 217}, {0, 241}, {0, 256}, {1, 336}, {0, 209}, {0, 352}, {0, 400}, {0, 368}, {1, 416}, {0, 320}, {0, 304}, {0, 288}, {0, 384}, {0, 432}, {0, 464}, {0, 448}, {1, 512}, {1, 576}, {0, 480}, {1, 704}, {0, 496}, {1, 672}, {0, 544}, {0, 608}, {0, 640}, {1, 864}, {1, 800}, {1, 768}, {1, 832}, {1, 736}, {1, 928}, {1, 960}, {1, 992}, {1, 1024}, {1, 896}}};  
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
