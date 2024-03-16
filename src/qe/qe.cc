#include "src/include/qe.h"
#include <sstream>

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

    std::vector<std::string> splitString(const std::string &str, char delimiter) {
        std::vector<std::string> result;
        std::istringstream iss(str);
        std::string token;

        while (std::getline(iss, token, delimiter)) {
            result.push_back(token);
        }

        return result;
    }

    int findAttributePosition(std::vector<Attribute>& attrs, std::string& lhsAttr){
        int position =0;
        for(const auto& attr:attrs){
            if(attr.name == lhsAttr){
                return position;
            }
            position++;
        }
        return position;
    }

    static int getActualByteForNullsIndicator(unsigned fieldCount) {

        return ceil((double) fieldCount / CHAR_BIT);
    }

    Filter::Filter(Iterator *input, const Condition &condition)
            : input(input), condition(condition){

        compOp = condition.op;
        std::vector<std::string> splitStrings = splitString(condition.lhsAttr, '.');
        tableName = splitStrings[0];
        lhsAttr = splitStrings[1];
        RelationManager::instance().getAttributes(tableName, classAttrs);
        attrPosition = findAttributePosition(classAttrs, lhsAttr);
        rhsValue = condition.rhsValue;

    }

    Filter::~Filter() {

    }

    RC readDeserializedAttrValue (char *inBuffer, int position, std::vector<Attribute> attrs, void *&attrValue) {
        int bitVectorSize = getActualByteForNullsIndicator (attrs.size());
        std::vector<bool> isNull;
        char *bitVectorPointer = inBuffer;
        AttrType  attrType = attrs[position].type;

        for (int i = 0; i < bitVectorSize; i++) {
            for (int bit = CHAR_BIT - 1; bit >= 0; --bit) {
                isNull.push_back((((char*)bitVectorPointer)[i] & (1 << bit)) != 0);
            }
        }

        if(isNull[position])
            return -1;

        char *recordOffset = inBuffer;
        recordOffset+= bitVectorSize;
        for(int i=0;i<position;i++) {
            switch (attrs[i].type) {
                case TypeInt:
                    if (!isNull[i])
                        recordOffset += sizeof(int);
                    break;
                case TypeReal:
                    if (!isNull[i])
                        recordOffset += sizeof(float);
                    break;
                case TypeVarChar:
                    if (!isNull[i]) {
                        int strLen = *(int *) (recordOffset);
                        recordOffset += sizeof(int) + strLen;
                    }
                    break;
            }
        }

        switch(attrType){
            case TypeInt:
                attrValue = malloc(sizeof (int));
                memcpy(attrValue, recordOffset, sizeof(int));
                break;
            case TypeReal:
                attrValue = malloc(sizeof (float));
                memcpy(attrValue, recordOffset, sizeof(float));
                break;
            case TypeVarChar:
                int strLen = *(int*)(recordOffset);
                attrValue = malloc(sizeof (int)+strLen);
                memcpy(attrValue, recordOffset, sizeof(int)+strLen);
                break;
        }



        return 0;
    }

    RC Filter::getNextTuple(void *data) {

        if(attrPosition == -1  || attrPosition >= classAttrs.size()){
            std::cout<<"Attribute not found in table"<<std::endl;
            return -1;
        }

        void *attrValue = nullptr;

        while (input->getNextTuple(data) != QE_EOF) {
            readDeserializedAttrValue((char*)data, attrPosition, classAttrs, attrValue);
            if(RecordBasedFileManager::instance().compareAttributes(attrValue, rhsValue.data, compOp, rhsValue.type)==0){
                return 0;
            }

        }
        return -1;
    }

    RC Filter::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        attrs = this->classAttrs;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        for (Attribute &attr : attrs) {
            attr.name = tableName + "." + attr.name;
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
        // table scan on left table for num
        // store in unordered map of size
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
