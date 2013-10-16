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
      return nullptr;
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

  int lookup[7][121][2] = {{{3116, 1}, {1052, 3}, {1475, 2}, {1112, 4}, {780, 6}, {846, 7}, {856, 8}, {1084, 5}, {661, 12}, {881, 11}, {850, 9}, {831, 10}, {655, 14}, {691, 15}, {678, 13}, {704, 16}, {755, 17}, {776, 18}, {752, 20}, {760, 19}, {784, 21}, {800, 22}, {759, 23}, {620, 24}, {665, 25}, {654, 33}, {714, 26}, {682, 27}, {787, 31}, {718, 28}, {644, 29}, {584, 38}, {716, 30}, {668, 32}, {564, 39}, {608, 34}, {668, 35}, {561, 36}, {622, 37}, {508, 42}, {494, 40}, {158, 49}, {551, 41}, {490, 43}, {175, 52}, {483, 45}, {499, 44}, {445, 46}, {157, 51}, {168, 48}, {417, 47}, {170, 50}, {144, 55}, {159, 53}, {140, 57}, {157, 56}, {149, 54}, {161, 59}, {157, 60}, {149, 64}, {134, 61}, {148, 62}, {129, 65}, {153, 63}, {151, 58}, {109, 73}, {107, 81}, {96, 89}, {62, 97}, {58, 105}, {53, 113}, {53, 121}, {53, 129}, {48, 137}, {45, 145}, {46, 153}, {43, 161}, {38, 169}, {36, 177}, {37, 185}, {27, 193}, {25, 201}, {27, 217}, {29, 209}, {25, 233}, {26, 225}, {26, 241}, {24, 249}, {23, 256}, {22, 272}, {22, 288}, {20, 304}, {20, 320}, {19, 336}, {16, 352}, {18, 368}, {13, 384}, {12, 400}, {12, 416}, {13, 432}, {11, 448}, {12, 464}, {11, 480}, {11, 496}, {9, 512}, {10, 544}, {10, 576}, {8, 608}, {9, 640}, {7, 672}, {8, 704}, {7, 736}, {5, 768}, {5, 800}, {5, 832}, {5, 864}, {5, 896}, {4, 928}, {4, 960}, {5, 992}, {4, 1024}},
  {{529, 3}, {524, 4}, {1541, 1}, {485, 5}, {741, 2}, {386, 6}, {310, 12}, {383, 7}, {366, 10}, {389, 8}, {403, 9}, {377, 11}, {306, 13}, {312, 16}, {321, 17}, {296, 15}, {301, 14}, {308, 21}, {292, 18}, {303, 19}, {323, 22}, {308, 20}, {278, 23}, {117, 24}, {99, 25}, {102, 26}, {102, 27}, {100, 28}, {118, 29}, {114, 32}, {115, 30}, {112, 31}, {109, 34}, {133, 35}, {130, 33}, {139, 36}, {138, 37}, {118, 44}, {127, 40}, {127, 41}, {130, 39}, {137, 38}, {120, 42}, {118, 45}, {68, 48}, {61, 53}, {106, 46}, {105, 47}, {72, 49}, {65, 50}, {125, 43}, {66, 51}, {62, 55}, {64, 52}, {65, 54}, {61, 58}, {59, 56}, {56, 60}, {63, 57}, {57, 59}, {52, 61}, {50, 63}, {52, 64}, {54, 62}, {50, 65}, {48, 73}, {44, 81}, {42, 89}, {29, 97}, {28, 105}, {26, 113}, {24, 121}, {24, 129}, {20, 137}, {22, 145}, {20, 153}, {20, 161}, {20, 169}, {19, 177}, {18, 185}, {12, 193}, {15, 201}, {13, 209}, {12, 217}, {12, 225}, {12, 233}, {11, 241}, {11, 249}, {11, 256}, {10, 272}, {10, 288}, {10, 304}, {9, 320}, {7, 336}, {7, 352}, {7, 368}, {5, 384}, {6, 400}, {6, 416}, {5, 432}, {5, 448}, {5, 464}, {4, 480}, {4, 496}, {4, 512}, {4, 544}, {4, 576}, {4, 608}, {4, 640}, {3, 672}, {3, 704}, {3, 736}, {2, 768}, {2, 800}, {2, 832}, {2, 864}, {2, 896}, {2, 928}, {2, 960}, {2, 992}, {2, 1024}},
  {{361, 1}, {168, 2}, {115, 4}, {110, 3}, {85, 5}, {77, 6}, {71, 7}, {65, 9}, {69, 8}, {48, 10}, {50, 13}, {55, 12}, {38, 17}, {53, 11}, {40, 16}, {47, 14}, {35, 18}, {46, 15}, {33, 19}, {25, 20}, {25, 21}, {27, 23}, {25, 22}, {22, 24}, {23, 25}, {23, 28}, {23, 26}, {20, 30}, {21, 27}, {22, 29}, {22, 31}, {23, 33}, {22, 32}, {23, 34}, {22, 37}, {16, 39}, {24, 35}, {23, 36}, {21, 38}, {14, 40}, {14, 42}, {13, 45}, {12, 49}, {13, 43}, {13, 44}, {15, 41}, {11, 50}, {12, 48}, {12, 47}, {12, 46}, {12, 53}, {10, 51}, {11, 52}, {11, 56}, {10, 57}, {12, 55}, {10, 59}, {9, 63}, {10, 54}, {10, 62}, {10, 58}, {9, 60}, {10, 61}, {9, 64}, {9, 65}, {6, 81}, {8, 73}, {5, 89}, {7, 97}, {4, 105}, {4, 121}, {5, 113}, {4, 129}, {4, 137}, {3, 161}, {4, 145}, {4, 153}, {4, 169}, {4, 177}, {3, 185}, {4, 193}, {3, 201}, {4, 209}, {3, 217}, {3, 225}, {4, 233}, {3, 249}, {3, 241}, {3, 256}, {3, 272}, {3, 288}, {3, 304}, {2, 320}, {2, 336}, {2, 352}, {2, 368}, {2, 384}, {2, 400}, {2, 416}, {2, 432}, {2, 448}, {2, 464}, {2, 480}, {2, 496}, {2, 512}, {1, 544}, {1, 576}, {1, 608}, {1, 640}, {1, 672}, {1, 704}, {1, 736}, {1, 768}, {1, 800}, {1, 832}, {1, 864}, {1, 896}, {1, 928}, {1, 960}, {1, 992}, {1, 1024}},
  {{55, 3}, {180, 1}, {83, 2}, {40, 5}, {33, 7}, {35, 6}, {66, 4}, {29, 9}, {30, 8}, {26, 11}, {22, 15}, {23, 10}, {27, 12}, {26, 13}, {19, 17}, {23, 14}, {20, 16}, {18, 19}, {17, 18}, {13, 21}, {12, 20}, {12, 23}, {12, 24}, {13, 22}, {11, 25}, {11, 26}, {11, 27}, {12, 28}, {12, 29}, {13, 30}, {12, 32}, {13, 34}, {14, 31}, {14, 33}, {11, 37}, {11, 36}, {12, 35}, {10, 38}, {7, 39}, {6, 41}, {7, 40}, {7, 42}, {6, 43}, {7, 44}, {6, 45}, {5, 47}, {7, 46}, {5, 48}, {6, 50}, {6, 51}, {6, 49}, {6, 54}, {6, 58}, {5, 52}, {5, 53}, {6, 61}, {6, 55}, {6, 57}, {5, 60}, {5, 56}, {4, 65}, {5, 59}, {5, 63}, {6, 62}, {5, 64}, {5, 73}, {3, 81}, {4, 89}, {4, 97}, {3, 105}, {4, 113}, {3, 121}, {4, 129}, {4, 137}, {3, 145}, {2, 153}, {2, 161}, {2, 169}, {2, 177}, {2, 185}, {2, 193}, {2, 209}, {2, 201}, {2, 217}, {1, 225}, {1, 241}, {1, 233}, {2, 249}, {1, 256}, {1, 272}, {1, 288}, {1, 304}, {1, 320}, {1, 336}, {1, 352}, {1, 368}, {1, 384}, {1, 400}, {1, 416}, {1, 432}, {1, 448}, {1, 464}, {1, 480}, {1, 496}, {1, 512}, {1, 544}, {1, 576}, {1, 608}, {1, 640}, {1, 672}, {1, 704}, {1, 736}, {1, 768}, {1, 800}, {1, 832}, {1, 864}, {1, 896}, {1, 928}, {1, 960}, {1, 992}, {1, 1024}},
  {{14, 2}, {12, 3}, {27, 1}, {6, 7}, {10, 4}, {8, 5}, {4, 9}, {7, 6}, {5, 12}, {5, 8}, {4, 10}, {4, 13}, {4, 14}, {5, 15}, {4, 17}, {3, 18}, {4, 22}, {3, 21}, {4, 23}, {4, 20}, {3, 19}, {4, 24}, {4, 11}, {4, 27}, {3, 28}, {2, 32}, {4, 30}, {4, 29}, {2, 33}, {2, 31}, {4, 16}, {2, 37}, {4, 25}, {2, 35}, {2, 34}, {2, 38}, {2, 41}, {2, 42}, {2, 40}, {3, 26}, {2, 51}, {1, 54}, {1, 56}, {1, 61}, {1, 52}, {2, 55}, {1, 65}, {2, 39}, {2, 36}, {2, 44}, {1, 50}, {2, 43}, {1, 46}, {2, 49}, {2, 45}, {1, 47}, {2, 48}, {1, 62}, {1, 73}, {1, 53}, {1, 58}, {1, 59}, {1, 60}, {1, 57}, {1, 63}, {1, 64}, {1, 81}, {0, 169}, {1, 89}, {1, 97}, {1, 105}, {1, 113}, {0, 145}, {0, 121}, {0, 129}, {1, 249}, {0, 137}, {0, 153}, {0, 161}, {1, 241}, {0, 177}, {0, 185}, {0, 217}, {0, 193}, {0, 384}, {0, 201}, {0, 209}, {0, 336}, {0, 352}, {1, 416}, {1, 432}, {0, 225}, {0, 272}, {0, 233}, {1, 448}, {1, 464}, {0, 480}, {0, 496}, {0, 256}, {0, 512}, {1, 544}, {0, 288}, {1, 576}, {1, 608}, {0, 304}, {1, 640}, {0, 320}, {0, 672}, {0, 400}, {0, 368}, {0, 736}, {1, 704}, {1, 768}, {1, 832}, {1, 800}, {1, 864}, {1, 896}, {1, 928}, {1, 960}, {1, 992}, {0, 1024}},
  {{6, 3}, {4, 5}, {13, 1}, {7, 2}, {4, 4}, {2, 14}, {1, 20}, {3, 6}, {2, 8}, {3, 7}, {1, 22}, {1, 24}, {1, 27}, {2, 9}, {2, 10}, {2, 11}, {2, 13}, {2, 12}, {2, 15}, {1, 16}, {1, 17}, {1, 21}, {1, 18}, {1, 19}, {1, 23}, {1, 25}, {1, 26}, {1, 28}, {1, 31}, {1, 29}, {1, 30}, {1, 33}, {1, 34}, {1, 36}, {1, 35}, {1, 39}, {1, 37}, {1, 32}, {1, 40}, {1, 43}, {1, 44}, {1, 38}, {1, 41}, {1, 46}, {1, 42}, {0, 47}, {1, 45}, {1, 49}, {0, 51}, {0, 50}, {0, 48}, {0, 52}, {0, 53}, {0, 54}, {0, 55}, {0, 56}, {0, 57}, {0, 58}, {0, 60}, {0, 63}, {0, 59}, {0, 61}, {0, 62}, {0, 64}, {0, 65}, {0, 73}, {0, 81}, {0, 89}, {0, 97}, {0, 105}, {0, 113}, {0, 121}, {0, 129}, {0, 137}, {0, 145}, {0, 153}, {0, 161}, {0, 169}, {0, 177}, {0, 185}, {0, 193}, {0, 201}, {0, 209}, {0, 217}, {0, 432}, {0, 225}, {0, 233}, {0, 249}, {0, 241}, {0, 256}, {0, 272}, {0, 368}, {0, 288}, {1, 480}, {0, 304}, {1, 544}, {0, 320}, {0, 464}, {1, 576}, {0, 336}, {1, 672}, {0, 384}, {0, 352}, {1, 608}, {1, 640}, {1, 800}, {0, 400}, {0, 416}, {0, 768}, {1, 832}, {1, 704}, {0, 448}, {0, 496}, {1, 896}, {1, 736}, {1, 928}, {0, 512}, {0, 864}, {1, 992}, {1, 1024}, {1, 960}},
  {{2, 1}, {1, 3}, {1, 2}, {0, 6}, {0, 5}, {1, 4}, {0, 7}, {0, 9}, {0, 12}, {0, 8}, {0, 13}, {0, 10}, {0, 11}, {0, 15}, {0, 14}, {0, 16}, {0, 17}, {0, 18}, {0, 20}, {0, 22}, {0, 19}, {0, 21}, {0, 24}, {0, 25}, {0, 23}, {0, 26}, {0, 29}, {0, 28}, {0, 27}, {0, 31}, {0, 35}, {0, 33}, {0, 34}, {0, 32}, {0, 30}, {0, 36}, {0, 37}, {0, 39}, {0, 40}, {0, 38}, {0, 42}, {0, 44}, {0, 41}, {0, 43}, {0, 46}, {0, 45}, {0, 50}, {0, 48}, {0, 49}, {0, 47}, {0, 53}, {0, 51}, {0, 54}, {0, 52}, {0, 57}, {0, 56}, {0, 55}, {0, 60}, {0, 59}, {0, 62}, {0, 61}, {0, 64}, {0, 58}, {0, 63}, {0, 65}, {0, 81}, {0, 73}, {0, 89}, {0, 97}, {0, 105}, {0, 113}, {0, 121}, {0, 129}, {0, 137}, {0, 161}, {0, 145}, {0, 153}, {0, 169}, {0, 177}, {0, 185}, {0, 193}, {0, 201}, {0, 209}, {0, 217}, {0, 225}, {0, 233}, {0, 241}, {0, 249}, {0, 256}, {0, 272}, {0, 304}, {0, 288}, {0, 320}, {1, 576}, {1, 336}, {0, 384}, {0, 352}, {0, 368}, {1, 416}, {0, 464}, {0, 400}, {1, 704}, {0, 640}, {1, 800}, {0, 432}, {0, 480}, {1, 736}, {1, 512}, {1, 768}, {0, 448}, {1, 928}, {1, 992}, {1, 832}, {0, 496}, {1, 864}, {1, 1024}, {0, 544}, {1, 672}, {1, 896}, {0, 608}, {1, 960}}};
  
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
