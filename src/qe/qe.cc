#include "src/include/qe.h"

namespace PeterDB {

    void computeParams(const Condition &condition, void* lowKey, void* highKey, bool &highKeyInclusive, bool &lowKeyInclusive){
        switch(condition.op){
            case LE_OP:
                highKeyInclusive = true;
                highKey = condition.rhsValue.data;
                lowKey = nullptr;
                break;
            case LT_OP:
                highKeyInclusive = false;
                highKey = condition.rhsValue.data;
                lowKey = nullptr;
                break;
            case GE_OP:
                lowKeyInclusive = true;
                lowKey = condition.rhsValue.data;
                highKey = nullptr;
                break;
            case GT_OP:
                lowKeyInclusive = false;
                lowKey = condition.rhsValue.data;
                highKey = nullptr;
                break;
            case EQ_OP:
                lowKey = condition.rhsValue.data;
                highKey = condition.rhsValue.data;
                lowKeyInclusive = true;
                highKeyInclusive = true;
                break;
        }
    }

    Filter::Filter(Iterator *input, const Condition &condition)
            : input(input), condition(condition){

        compOp = condition.op;
        lhsAttr = condition.lhsAttr;
        rhsValue = condition.rhsValue;

        if (dynamic_cast<TableScan*>(input)) {

            tableScan = dynamic_cast<TableScan*>(input);
            tableName = tableScan->getTableName();
            std::vector<std::string> attrNames;
            tableScan->getAttributesVanilla(attrNames);

            tableScan->setIterator(compOp, lhsAttr, rhsValue.data, attrNames);

        }
        else{

            indexScan = dynamic_cast<IndexScan*>(input);

            void *lowKey, *highKey;
            bool lowKeyInclusive, highKeyInclusive;

            computeParams(condition, lowKey, highKey, lowKeyInclusive, highKeyInclusive);
            indexScan->setIterator(lowKey, highKey, lowKeyInclusive, highKeyInclusive);

        }
    }

    Filter::~Filter() {

    }

    RC Filter::getNextTuple(void *data) {
        if (dynamic_cast<TableScan*>(input)) {
            tableScan->getNextTuple(data);
        }
        else if(dynamic_cast<IndexScan*>(input)) {
            indexScan->getNextTuple(data);
        }
        else{
            return -1;
        }
        return 0;
    }

    RC Filter::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        if (dynamic_cast<TableScan*>(input)) {
            tableScan->getAttributes(attrs);
        }
        else if(dynamic_cast<IndexScan*>(input)) {
            indexScan->getAttributes(attrs);
        }
        else{
            return -1;
        }
        return 0;
    }

    Project::Project(Iterator *input, const std::vector<std::string> &attrNames) {

    }

    Project::~Project() {

    }

    RC Project::getNextTuple(void *data) {
        return -1;
    }

    RC Project::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned int numPages) {

    }

    BNLJoin::~BNLJoin() {

    }

    RC BNLJoin::getNextTuple(void *data) {
        return -1;
    }

    RC BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {

    }

    INLJoin::~INLJoin() {

    }

    RC INLJoin::getNextTuple(void *data) {
        return -1;
    }

    RC INLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned int numPartitions) {

    }

    GHJoin::~GHJoin() {

    }

    RC GHJoin::getNextTuple(void *data) {
        return -1;
    }

    RC GHJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op) {

    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op) {

    }

    Aggregate::~Aggregate() {

    }

    RC Aggregate::getNextTuple(void *data) {
        return -1;
    }

    RC Aggregate::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }
} // namespace PeterDB
