//
// Created by harper on 4/6/20.
//

#include <parquet/types.h>
#include <lqf/data_model.h>
#include <lqf/filter.h>
#include <lqf/agg.h>
#include <lqf/sort.h>
#include <lqf/print.h>
#include <lqf/join.h>
#include <lqf/mat.h>
#include <lqf/union.h>
#include "tpchquery.h"


namespace lqf {
    namespace tpch {

        using namespace agg;
        using namespace sboost;
        namespace q19 {
            ByteArray brand1("Brand#12");
            ByteArray brand2("Brand#23");
            ByteArray brand3("Brand#34");

            ByteArray shipmentInst("DELIVER IN PERSON");
            ByteArray shipMode1("AIR");
            ByteArray shipMode2("REG AIR");


            class PriceField : public DoubleSum {
            public:
                PriceField() : DoubleSum(LineItem::EXTENDEDPRICE) {}

                void reduce(DataRow &row) override {
                    *value_ += row[LineItem::EXTENDEDPRICE].asDouble() * (1 - row[LineItem::DISCOUNT].asDouble());
                }
            };
        }
        using namespace q19;

        void executeQ19() {
            auto lineitem = ParquetTable::Open(LineItem::path, {LineItem::PARTKEY, LineItem::QUANTITY,
                                                                LineItem::SHIPMODE, LineItem::SHIPINSTRUCT,
                                                                LineItem::EXTENDEDPRICE, LineItem::DISCOUNT});
            auto part = ParquetTable::Open(Part::path, {Part::PARTKEY, Part::BRAND, Part::CONTAINER, Part::SIZE});

            ColFilter lineitemBaseFilter({new SboostPredicate<ByteArrayType>(LineItem::SHIPINSTRUCT,
                                                                             bind(&ByteArrayDictEq::build,
                                                                                  shipmentInst)),
                                          new SboostPredicate<ByteArrayType>(LineItem::SHIPMODE,
                                                                             bind(&ByteArrayDictMultiEq::build,
                                                                                  [](const ByteArray &val) {
                                                                                      return val == shipMode1 ||
                                                                                             val == shipMode2;
                                                                                  }))});

            ColFilter lineitemFilter1({new SboostPredicate<Int32Type>(LineItem::QUANTITY,
                                                                      bind(&Int32DictBetween::build, 1,
                                                                           11))});
            auto qtyLineitem1 = lineitemFilter1.filter(*lineitem);

            ColFilter lineitemFilter2({new SboostPredicate<Int32Type>(LineItem::QUANTITY,
                                                                      bind(&Int32DictBetween::build, 10, 20))});
            auto qtyLineitem2 = lineitemFilter2.filter(*lineitem);

            ColFilter lineitemFilter3({new SboostPredicate<Int32Type>(LineItem::QUANTITY,
                                                                      bind(&Int32DictBetween::build, 20, 30))});
            auto qtyLineitem3 = lineitemFilter3.filter(*lineitem);

            FilterMat fmat;
            auto lineitemBase = fmat.mat(*lineitemBaseFilter.filter(*lineitem));

            FilterAnd fand;
            auto validLineitem1 = fand.execute({qtyLineitem1.get(), lineitemBase.get()});
            auto validLineitem2 = fand.execute({qtyLineitem2.get(), lineitemBase.get()});
            auto validLineitem3 = fand.execute({qtyLineitem3.get(), lineitemBase.get()});

            ColFilter partFilter1(
                    {new SboostPredicate<ByteArrayType>(Part::BRAND, bind(&ByteArrayDictEq::build, brand1)),
                     new SboostPredicate<Int32Type>(Part::SIZE, bind(&Int32DictBetween::build, 1, 5)),
                     new SboostPredicate<ByteArrayType>(Part::CONTAINER, bind(&ByteArrayDictMultiEq::build,
                                                                              [](const ByteArray &val) {
                                                                                  return val == ByteArray("SM CASE") ||
                                                                                         val == ByteArray("SM BOX") ||
                                                                                         val == ByteArray("SM PACK") ||
                                                                                         val == ByteArray("SM PKG");
                                                                              }))});
            auto validPart1 = partFilter1.filter(*part);

            ColFilter partFilter2(
                    {new SboostPredicate<ByteArrayType>(Part::BRAND, bind(&ByteArrayDictEq::build, brand2)),
                     new SboostPredicate<Int32Type>(Part::SIZE, bind(&Int32DictBetween::build, 1, 10)),
                     new SboostPredicate<ByteArrayType>(Part::CONTAINER, bind(&ByteArrayDictMultiEq::build,
                                                                              [](const ByteArray &val) {
                                                                                  return val == ByteArray("MED BAG") ||
                                                                                         val == ByteArray("MED BOX") ||
                                                                                         val == ByteArray("MED PACK") ||
                                                                                         val == ByteArray("MED PKG");
                                                                              }))});
            auto validPart2 = partFilter2.filter(*part);

            ColFilter partFilter3(
                    {new SboostPredicate<ByteArrayType>(Part::BRAND, bind(&ByteArrayDictEq::build, brand3)),
                     new SboostPredicate<Int32Type>(Part::SIZE, bind(&Int32DictBetween::build, 1, 15)),
                     new SboostPredicate<ByteArrayType>(Part::CONTAINER, bind(&ByteArrayDictMultiEq::build,
                                                                              [](const ByteArray &val) {
                                                                                  return val == ByteArray("LG CASE") ||
                                                                                         val == ByteArray("LG BOX") ||
                                                                                         val == ByteArray("LG PACK") ||
                                                                                         val == ByteArray("LG PKG");
                                                                              }))});
            auto validPart3 = partFilter3.filter(*part);

            HashFilterJoin itemOnPartJoin1(LineItem::PARTKEY, Part::PARTKEY);
            auto itemWithPart1 = itemOnPartJoin1.join(*validLineitem1, *validPart1);

            HashFilterJoin itemOnPartJoin2(LineItem::PARTKEY, Part::PARTKEY);
            auto itemWithPart2 = itemOnPartJoin2.join(*validLineitem2, *validPart2);

            HashFilterJoin itemOnPartJoin3(LineItem::PARTKEY, Part::PARTKEY);
            auto itemWithPart3 = itemOnPartJoin3.join(*validLineitem3, *validPart3);

            FilterUnion funion;
            auto unioneditem = funion.execute({itemWithPart1.get(), itemWithPart2.get(), itemWithPart3.get()});


            SimpleAgg sumagg({1}, []() { return vector<AggField *>{new PriceField()}; });
            auto result = sumagg.agg(*unioneditem);

            Printer printer(PBEGIN PD(0) PEND);
            printer.print(*result);
        }
    }
}