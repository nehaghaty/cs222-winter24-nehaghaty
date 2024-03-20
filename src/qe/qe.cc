#include "src/include/qe.h"
#include <sstream>
#include <cstring>
#include <cmath>

#define CHAR_BIT    8

namespace PeterDB {

    std::vector<std::string> splitString(const std::string &str, char delimiter) {
        std::vector<std::string> result;
        std::istringstream iss(str);
        std::string token;

        while (std::getline(iss, token, delimiter)) {
            result.push_back(token);
        }

        return result;
    }

    int findAttributePosition(std::vector<Attribute>& attrs, const std::string& lhsAttr){
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
//        std::vector<std::string> splitStrings = splitString(condition.lhsAttr, '.');
//        tableName = splitStrings[0];
//        lhsAttr = splitStrings[1];
//        RelationManager::instance().getAttributes(tableName, classAttrs);
        input->getAttributes(classAttrs);
        attrPosition = findAttributePosition(classAttrs, condition.lhsAttr);
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
        return 0;
    }

    //will get a deser record which has the whole record
    // some attribute names will be given which have to be selected in that order
    // first select their null bits and make a null bit vector
    // then select the given attributes - use readattribute by neha to get a particular attribute's value
    // keep appending the values

    RC projectSelectedAttrs(char *record,
                            const std::vector<Attribute>&recordDescriptor,
                            const std::vector<std::string>&attributeNames,
                            void *&data,
                            std::unordered_map<std::string, int> &attributePositions,
                            std::vector<std::string> projectedPureAttrs,
                            std::unordered_set<int> positionsRequired) {

        char *bitVectorPointer = record;
        int fieldCount = recordDescriptor.size();
        int originalBitVectorSize = ceil((double) fieldCount / CHAR_BIT);
        int newBitVectorSize = ceil((double) attributeNames.size() / CHAR_BIT);
        std::vector<bool> isNull;

        for (int i = 0; i < originalBitVectorSize; i++) {
            for (int bit = CHAR_BIT - 1; bit >= 0; --bit) {
                isNull.push_back((((char*)bitVectorPointer)[i] & (1 << bit)) != 0);
            }
        }
        int total_size = newBitVectorSize;

        for (auto attribute: projectedPureAttrs) {
            int position = attributePositions[attribute];

            total_size += recordDescriptor[position].length;
            if (recordDescriptor[position].type == TypeVarChar)
                total_size += sizeof (int);
        }

        char* deserializedRecord = (char*)malloc(total_size);
        char *deSerRecordPointer = deserializedRecord;
        memset(deserializedRecord, 0, total_size);

        char *bitvector;
        RecordBasedFileManager::instance().
        processSelectedAttributes (attributeNames, attributePositions, bitvector, isNull);

        memcpy(deSerRecordPointer, bitvector, newBitVectorSize);
        free(bitvector);

        deSerRecordPointer += newBitVectorSize;

        //TODO: allocate only as much is required
        for (int i = 0; i < projectedPureAttrs.size(); i ++) {
            int position = attributePositions[projectedPureAttrs[i]];

            if (isNull[position]) {
                continue;
            }

            void *inBuffer = nullptr;
            readDeserializedAttrValue (record, position, recordDescriptor, inBuffer);
            char *attributeValue = (char*)inBuffer;

            if (recordDescriptor[position].type == TypeVarChar) {
                total_size -= recordDescriptor[position].length;
                int length_of_string = *(int*)attributeValue;
                memcpy(deSerRecordPointer, &length_of_string, sizeof (int));
                deSerRecordPointer += sizeof (length_of_string);

                total_size += length_of_string;
                memcpy(deSerRecordPointer, attributeValue + sizeof (int), length_of_string);
                deSerRecordPointer += length_of_string;
            }
            else {
//                    copy real/int to project
                memcpy(deSerRecordPointer, attributeValue, sizeof (int));
                deSerRecordPointer += sizeof (int);
            }
        }
        memcpy(data, deserializedRecord , total_size);

        free(deserializedRecord);
        return 0;
    }

    Project::Project(Iterator *input, const std::vector<std::string> &attrNames)
                    : input(input), projectedAttrs(attrNames) {
        //TODO: Order of attributes ?

        tableName = splitString(attrNames[0], '.')[0];
        RelationManager::instance().getAttributes(tableName, actualAttributes);

        for (int i = 0; i < actualAttributes.size(); i++) {
            actualAttributePositions[actualAttributes[i].name] = i;
            actualAttrNames.push_back(actualAttributes[i].name);
        }

        for (std::string i: attrNames) {
            std::string attributeName = splitString(i, '.')[1];
            positionsRequired.insert(actualAttributePositions[attributeName]);
            projectedPureAttrs.push_back(attributeName);
        }
    }

    Project::~Project() {

    }

    RC Project::getNextTuple(void *data) {

        void *outBuffer = malloc(PAGE_SIZE);
        memset(outBuffer, 0, PAGE_SIZE);
        if (input->getNextTuple(outBuffer) == IX_EOF)
            return IX_EOF;

        projectSelectedAttrs((char *) outBuffer,
                             actualAttributes,
                             actualAttrNames,
                             data,
                             actualAttributePositions,
                             projectedPureAttrs,
                             positionsRequired);

        char *out = (char*)data + 1;
        int c = *(float*)(out);
        int d = *(int*)(out + sizeof (int));

        return 0;
    }

    RC Project::getAttributes(std::vector<Attribute> &attributes) const {

        attributes.clear();
        // For attribute in std::vector<Attribute>, name it as rel.attr

        //TODO: Order of attributes ?
        for (std::string attr: projectedAttrs) {
            Attribute attribute;
            attribute.name = splitString(attr, '.')[1];
            if (actualAttributePositions.find(attribute.name) != actualAttributePositions.end()) {
                attribute = actualAttributes[actualAttributePositions.at(attribute.name)];
                attribute.name = tableName + "." + attribute.name;
                attributes.push_back(attribute);
            }
        }

        return 0;
    }

    int calculateTupleSize(void *inBuffer, std::vector<Attribute> attrs){

        int bitVectorSize = getActualByteForNullsIndicator (attrs.size());
        std::vector<bool> isNull;
        char *bitVectorPointer = (char*)inBuffer;

        for (int i = 0; i < bitVectorSize; i++) {
            for (int bit = CHAR_BIT - 1; bit >= 0; --bit) {
                isNull.push_back((((char*)bitVectorPointer)[i] & (1 << bit)) != 0);
            }
        }

        char *recordOffset = (char*)inBuffer;
        recordOffset+= bitVectorSize;
        int recordSize = bitVectorSize;

        for(int i=0;i<attrs.size();i++) {
            switch (attrs[i].type) {
                case TypeInt:
                    if (!isNull[i]){
                        recordSize += sizeof(int);
                        recordOffset += sizeof(int);
                    }
                    break;
                case TypeReal:
                    if (!isNull[i]){
                        recordSize += sizeof(int);
                        recordOffset += sizeof(float);
                    }
                    break;
                case TypeVarChar:
                    if (!isNull[i]){
                        int strLen = *(int *) (recordOffset);
                        recordSize += sizeof(int) + strLen;
                        recordOffset += sizeof(float );
                    }
                    break;
            }
        }

        return recordSize;
    }

    BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned int numPages)
    : leftIn(leftIn), rightIn(rightIn), condition(condition), numPages(numPages)
    {
        //create joined table
        joinAttrs.clear();

        leftIn->getAttributes(leftInAttrs);
        for(auto attr:leftInAttrs){
            if(attr.name == condition.lhsAttr){
                leftInAttr = attr;
            }
        }

        rightIn->getAttributes(rightInAttrs);
        for(auto attr:rightInAttrs){
            if(attr.name == condition.rhsAttr){
                rightInAttr = attr;
            }
        }

        joinAttrs.insert(joinAttrs.end(), leftInAttrs.begin(), leftInAttrs.end());
        joinAttrs.insert(joinAttrs.end(), rightInAttrs.begin(), rightInAttrs.end());

        leftAttrPos = findAttributePosition(leftInAttrs, leftInAttr.name);
        rightAttrPos = findAttributePosition(rightInAttrs, rightInAttr.name);


    }

    BNLJoin::~BNLJoin() {

    RC BNLJoin::insertIntoMap(void *tupleData, std::vector<Attribute> &leftAttrs, Attribute &condAttr, int leftAttrPos){
        void* leftAttrKey;
        readDeserializedAttrValue((char*)tupleData, leftAttrPos, leftAttrs, leftAttrKey);
        switch (condAttr.type) {
            case TypeInt: {
                int intKey = *(int *) leftAttrKey;
                auto intIt = intMap.find(intKey);
                if (intIt != intMap.end()) {
                    intIt->second.push_back(tupleData);
                } else {
                    intMap.insert(std::make_pair(intKey, std::vector<void *>{tupleData}));
                }
                break;
            }
            case TypeReal: {
                float floatKey = *(float *) leftAttrKey;
                auto floatIt = floatMap.find(floatKey);
                if (floatIt != floatMap.end()) {
                    floatIt->second.push_back(tupleData);
                } else {
                    floatMap.insert(std::make_pair(floatKey, std::vector<void *>{tupleData}));
                }
                break;
            }

            case TypeVarChar: {
                int keyLen = *(int *) leftAttrKey;
                std::string varcharString = std::string((char *) leftAttrKey + sizeof(int), keyLen);
                auto varcharIt = varcharMap.find(varcharString);
                if (varcharIt != varcharMap.end()) {
                    varcharIt->second.push_back(tupleData);
                } else {
                    varcharMap.insert(std::make_pair(varcharString, std::vector<void *>{tupleData}));
                }
                break;
            }
        }
        return 0;
    }

    void calculateSizeAndDoJoin(void* leftTuple, void* rightTuple, std::vector<Attribute> leftAttrs, std::vector<Attribute> rightAttrs, void* data){

        int leftNumFields = leftAttrs.size();
        int leftBitVecBytes = getActualByteForNullsIndicator (leftNumFields);

        int rightNumFields = rightAttrs.size();
        int rightBitVecBytes = getActualByteForNullsIndicator(rightNumFields);

        std::vector<bool> isNull;
        for (int i = 0; i < leftNumFields; i++) {
            for (int bit = CHAR_BIT - 1; bit >= 0; --bit) {
                isNull.push_back((((char*)leftTuple)[i] & (1 << bit)) != 0);
            }
        }
        for (int i = 0; i < rightNumFields; i++) {
            for (int bit = CHAR_BIT - 1; bit >= 0; --bit) {
                isNull.push_back((((char*)rightTuple)[i] & (1 << bit)) != 0);
            }
        }

        int newBitVectorBytes = (leftNumFields + rightNumFields + 7) / 8;
        char newBitVector[newBitVectorBytes];
        memset(newBitVector, 0, newBitVectorBytes);
        for (int i = 0; i < isNull.size(); ++i) {
            // Calculate destination position
            int destByteIndex = i / 8;
            int destBitIndex = i % 8;
            if (isNull[i]) {
                newBitVector[destByteIndex] |= (1 << (7 - destBitIndex));
            }
        }

        int leftTupleSize = calculateTupleSize(leftTuple, leftAttrs);
        int rightTupleSize = calculateTupleSize(rightTuple, rightAttrs);
        int newTupleSize = leftTupleSize + rightTupleSize + newBitVectorBytes - leftBitVecBytes - rightBitVecBytes;
        char newTuple[newTupleSize];
        char *newTupleOffset = newTuple;
        memcpy(newTupleOffset, newBitVector, newBitVectorBytes);
        newTupleOffset += newBitVectorBytes;
        memcpy(newTupleOffset, (char *) leftTuple + leftBitVecBytes, leftTupleSize - leftBitVecBytes);
        newTupleOffset += leftTupleSize-leftBitVecBytes;
        memcpy(newTupleOffset, (char *) rightTuple + rightBitVecBytes, rightTupleSize - rightBitVecBytes);
        memcpy(data, newTuple, newTupleSize);
    }

    RC BNLJoin::joinTables(void* data, char* rightTuple, void* rightAttrVal) {

        switch (rightInAttr.type) {
            case TypeInt:
            {
                int intKey = *(int *) rightAttrVal;
                auto intIt = intMap.find(intKey);
                if (intIt != intMap.end()) {
                    std::vector<void *> tupleVector = intIt->second;
                    for (auto leftTuple: tupleVector) {
                        calculateSizeAndDoJoin(leftTuple, rightTuple, rightInAttrs, leftInAttrs, data);
//                        std::cout<<"joined table:"<<std::endl;
                        RelationManager::instance().printTuple(joinAttrs, data, std::cout);
                        std::cout<<std::endl;
                    }
                    return 0;
                }
                else{
                    return -1;
                }
            }
            case TypeReal:
            {
                float floatKey = *(float *) rightAttrVal;
                auto floatIt = floatMap.find(floatKey);
                if (floatIt != floatMap.end()) {
                    std::vector<void *> tupleVector = floatIt->second;
                    for (auto leftTuple: tupleVector) {
                        calculateSizeAndDoJoin(leftTuple, rightTuple, rightInAttrs, leftInAttrs, data);
                    }
                }
                break;
            }
            case TypeVarChar:
            {
                std::string varcharKey = (char *)rightAttrVal;
                auto varcharIt = varcharMap.find(varcharKey);
                if (varcharIt != varcharMap.end()) {
                    std::vector<void *> tupleVector = varcharIt->second;
                    for (auto leftTuple: tupleVector) {
                        calculateSizeAndDoJoin(leftTuple, rightTuple, rightInAttrs, leftInAttrs, data);
                    }
                }
                break;
            }
        }
        return 0;
    }

    RC BNLJoin::getNextTuple(void *data) {
        // table scan on left table -> load all recs
        char page[PAGE_SIZE];
        while (leftIn->getNextTuple(page) != QE_EOF){
            int tupleSize = calculateTupleSize(page, leftInAttrs);
            void* tupleData = malloc(tupleSize);
            memcpy(tupleData, page, tupleSize);

            // store in unordered map
            insertIntoMap(tupleData, leftInAttrs, leftInAttr, leftAttrPos);
        }

        char rightTuple[PAGE_SIZE];
        while (rightIn->getNextTuple(rightTuple) != QE_EOF){

            void* rightAttrVal;
            readDeserializedAttrValue((char*)rightTuple, rightAttrPos,
                                      rightInAttrs, rightAttrVal);
            if(joinTables(data, rightTuple, rightAttrVal) == -1)
                continue;
            else{
                return 0;
            }
        }
        return QE_EOF;
    }

    RC BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs = joinAttrs;
        return 0;
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

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op):
            aggregateAttribute(aggAttr), op(op), input(input){
        minValue = 0;
        maxValue = 0;
        valueSum = 0;
        valueCount = 0;
        returned = false;
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op) {

    }

    Aggregate::~Aggregate() {

    }

    RC Aggregate::getNextTuple(void *data) {
        void *attrValue = nullptr;
        std::vector<std::string> splitStrings = splitString(aggregateAttribute.name, '.');
        tableName = splitStrings[0];
        lhsAttr = splitStrings[1];

        RelationManager::instance().getAttributes(tableName, classAttrs);
        attrPosition = findAttributePosition(classAttrs, lhsAttr);

        while (input->getNextTuple(data) != QE_EOF) {
            readDeserializedAttrValue((char*)data, attrPosition, classAttrs, attrValue);
            float retrievedAttribute = *(int*)((char*)attrValue);
            maxValue = std::max(maxValue, retrievedAttribute);
            valueSum += retrievedAttribute;
            valueCount += 1;
        }
        if (returned)
            return QE_EOF;

        returned = true;
        memset(data, 0, 1);
        float toBeCopied = 0;
        switch (op) {
            case MAX:
                toBeCopied = maxValue;
                break;
            case MIN:
                toBeCopied = minValue;
                break;
            case AVG:
                toBeCopied = (valueSum / valueCount);
                break;
            case SUM:
                toBeCopied = valueSum;
                break;
            case COUNT:
                toBeCopied = valueCount;
        }
        memcpy((char*)data + 1, &toBeCopied, sizeof (maxValue));
        return 0;
    }

    RC Aggregate::getAttributes(std::vector<Attribute> &attrs) const {
        std::string opName;
        switch (op) {
            case MIN:
                opName = "MIN";
                break;
            case MAX:
                opName = "MAX";
                break;
            case COUNT:
                opName = "COUNT";
                break;
            case SUM:
                opName = "SUM";
                break;
            case AVG:
                opName = "AVG";
                break;
        }

        Attribute attr;
        attr.name = opName + "(" + aggregateAttribute.name + ")";
        attr.type = TypeReal;
        attr.length = sizeof(float);

        attrs.clear();
        attrs.push_back(attr);
        return 0;
    }
} // namespace PeterDB
