#ifndef _qe_h_
#define _qe_h_

#include <vector>
#include <string>
#include <limits>
#include <unordered_set>

#include "rm.h"
#include "ix.h"

namespace PeterDB {

#define QE_EOF (-1)  // end of the index scan
    typedef enum AggregateOp {
        MIN = 0, MAX, COUNT, SUM, AVG
    } AggregateOp;

    // The following functions use the following
    // format for the passed data.
    //    For INT and REAL: use 4 bytes
    //    For VARCHAR: use 4 bytes for the length followed by the characters

    typedef struct Value {
        AttrType type;          // type of value
        void *data;             // value
    } Value;

    typedef struct Condition {
        std::string lhsAttr;        // left-hand side attribute
        CompOp op;                  // comparison operator
        bool bRhsIsAttr;            // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
        std::string rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
        Value rhsValue;             // right-hand side value if bRhsIsAttr = FALSE
    } Condition;

    class Iterator {
        // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;

        virtual RC getAttributes(std::vector<Attribute> &attrs) const = 0;

        virtual ~Iterator() = default;
    };

    class TableScan : public Iterator {
        // A wrapper inheriting Iterator over RM_ScanIterator
    private:
        RelationManager &rm;
        RM_ScanIterator iter;
        std::string tableName;
        std::vector<Attribute> attrs;
        std::vector<std::string> attrNames;
        RID rid;
    public:
        TableScan(RelationManager &rm, const std::string &tableName, const char *alias = NULL) : rm(rm) {
            //Set members
            this->tableName = tableName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Get Attribute Names from RM
            for (const Attribute &attr : attrs) {
                // convert to char *
                attrNames.push_back(attr.name);
            }

            // Call RM scan to get an iterator
            rm.scan(tableName, "", NO_OP, NULL, attrNames, iter);

            // Set alias
            if (alias) this->tableName = alias;
        };

        // Start a new iterator given the new compOp and value
        void setIterator(CompOp compOp, const std::string &conditionAttribute, const void *value, std::vector<std::string> attrNames) {
            iter.close();
            rm.scan(tableName, conditionAttribute, compOp, value, attrNames, iter);
        };

        RC getNextTuple(void *data) override {
            return iter.getNextTuple(rid, data);
        };

        RC getAttributes(std::vector<Attribute> &attributes) const override {
            attributes.clear();
            attributes = this->attrs;

            // For attribute in std::vector<Attribute>, name it as rel.attr
            for (Attribute &attribute : attributes) {
                attribute.name = tableName + "." + attribute.name;
            }
            return 0;
        };

        RC getAttributesVanilla(std::vector<std::string> &attrNames) {
            attrNames.clear();

            for (Attribute &attribute : this->attrs) {
                attrNames.push_back(attribute.name);
            }
            return 0;
        };

        ~TableScan() override {
            iter.close();
        };
    };

    class IndexScan : public Iterator {
        // A wrapper inheriting Iterator over IX_IndexScan
    private:
        RelationManager &rm;
        RM_IndexScanIterator iter;
        std::string tableName;
        std::string attrName;
        std::vector<Attribute> attrs;
        char key[PAGE_SIZE];
        RID rid;
    public:
        IndexScan(RelationManager &rm, const std::string &tableName, const std::string &attrName,
                  const char *alias = NULL) : rm(rm) {
            // Set members
            this->tableName = tableName;
            this->attrName = attrName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Call rm indexScan to get iterator
            rm.indexScan(tableName, attrName, NULL, NULL, true, true, iter);

            // Set alias
            if (alias) this->tableName = alias;
        };

        // Start a new iterator given the new key range
        void setIterator(void *lowKey, void *highKey, bool lowKeyInclusive, bool highKeyInclusive) {
            iter.close();
            rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive, highKeyInclusive, iter);
        };

        RC getNextTuple(void *data) override {
            std::cout << __LINE__ << std::endl;
            RC rc = iter.getNextEntry(rid, key);
            if (rc == 0) {
                rc = rm.readTuple(tableName, rid, data);
                std::vector<Attribute> attrs;
                rm.getAttributes(tableName, attrs);
//                rm.printTuple(attrs, data, std::cout);
            }
            return rc;
        };

        RC getAttributes(std::vector<Attribute> &attributes) const override {
            attributes.clear();
            attributes = this->attrs;

            // For attribute in std::vector<Attribute>, name it as rel.attr
            for (Attribute &attribute : attributes) {
                attribute.name = tableName + "." + attribute.name;
            }
            return  0;
        };

        ~IndexScan() override {
            iter.close();
        };
    };

    class Filter : public Iterator {
        CompOp compOp;
        std::string lhsAttr;
        Value rhsValue;
        Iterator *input;
        Condition condition;
        std::string tableName;
        int attrPosition;
        std::vector <Attribute> classAttrs;
        // Filter operator
    public:
        Filter(Iterator *input,               // Iterator of input R
               const Condition &condition     // Selection condition
        );

        ~Filter() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };

    class Project : public Iterator {
        std::string lhsAttr;
        Iterator *input;
        TableScan *tableScan;
        IndexScan *indexScan;
        std::string tableName;
        std::vector<std::string> projectedAttrs;
        std::vector<std::string> projectedPureAttrs;
        std::vector<Attribute> actualAttributes;
        std::vector<std::string> actualAttrNames;
        std::unordered_map<std::string, int> actualAttributePositions;
        std::unordered_set<int> positionsRequired;

    public:
        Project(Iterator *input,                                // Iterator of input R
                const std::vector<std::string> &attrNames);     // std::vector containing attribute names
        ~Project() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };

    typedef std::vector<void*> RecordList;

    class BNLJoin : public Iterator {
        // Block nested-loop join operator
        Iterator *leftIn;
        TableScan *rightIn;
        Condition condition;
        unsigned int numPages;
        Attribute leftInAttr;
        Attribute rightInAttr;
        int leftAttrPos;
        int rightAttrPos;
        std::vector<Attribute> leftInAttrs;
        std::vector<Attribute> rightInAttrs;
        std::vector<Attribute> joinAttrs;
        std::unordered_map<int, std::vector<void*>> intMap;
        std::unordered_map<float, std::vector<void*>> floatMap;
        std::unordered_map<std::string, std::vector<void*>> varcharMap;

    public:
        BNLJoin(Iterator *leftIn,            // Iterator of input R
                TableScan *rightIn,           // TableScan Iterator of input S
                const Condition &condition,   // Join condition
                const unsigned numPages       // # of pages that can be loaded into memory,
                //   i.e., memory block size (decided by the optimizer)
        );

        ~BNLJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;

        RC insertIntoMap(void *tupleData, std::vector<Attribute> &leftAttrs, Attribute &condAttr, int leftAttrPos);

        RC joinTables(void* data, char rightTuple[PAGE_SIZE], void* rightAttrVal);
    };

    class INLJoin : public Iterator {

    public:
        INLJoin(Iterator *leftIn,           // Iterator of input R
                IndexScan *rightIn,          // IndexScan Iterator of input S
                const Condition &condition   // Join condition
        );

        ~INLJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };

    // 10 extra-credit points
    class GHJoin : public Iterator {
        // Grace hash join operator

    public:
        GHJoin(Iterator *leftIn,               // Iterator of input R
               Iterator *rightIn,               // Iterator of input S
               const Condition &condition,      // Join condition (CompOp is always EQ)
               const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
        );

        ~GHJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };

    class Aggregate : public Iterator {
        // Aggregation operator
    public:
        // Mandatory
        // Basic aggregation
        float minValue;
        float maxValue;
        float valueSum;
        float valueCount;
        bool returned;

        std::string lhsAttr;
        Value rhsValue;
        Iterator *input;
        std::string tableName;
        int attrPosition;
        std::vector <Attribute> classAttrs;
        Attribute aggregateAttribute;
        AggregateOp op;

        Aggregate(Iterator *input,          // Iterator of input R
                  const Attribute &aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        );

        // Optional for everyone: 5 extra-credit points
        // Group-based hash aggregation
        Aggregate(Iterator *input,             // Iterator of input R
                  const Attribute &aggAttr,           // The attribute over which we are computing an aggregate
                  const Attribute &groupAttr,         // The attribute over which we are grouping the tuples
                  AggregateOp op              // Aggregate operation
        );

        ~Aggregate() override;

        RC getNextTuple(void *data) override;

        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrName = "MAX(rel.attr)"
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };
} // namespace PeterDB

#endif // _qe_h_
