#include "src/include/rm.h"
#include <iostream>
#include <cstring>
#include <sys/stat.h>

namespace PeterDB {
    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }

    RelationManager::RelationManager() = default;

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    FileHandle tablesFileHandle;
    FileHandle attrsFileHandle;
    std::string tablesFileName = "Tables";
    std::string attrsFileName = "Columns";
    std::vector<PeterDB::Attribute> tablesRecordDescriptor;
    std::vector<PeterDB::Attribute> attrsRecordDescriptor;



    void addAttribute(std::vector<PeterDB::Attribute> &recordDescriptor,
                      const std::string &name,
                      PeterDB::AttrType type,
                      PeterDB::AttrLength length) {
        PeterDB::Attribute attr;
        attr.name = name;
        attr.type = type;
        attr.length = length;
        recordDescriptor.push_back(attr);
    }

    void createTableRecordDescriptor(std::vector<PeterDB::Attribute> &recordDescriptor) {
        addAttribute(recordDescriptor, "Id", PeterDB::TypeInt, 4);
        addAttribute(recordDescriptor, "Name", PeterDB::TypeVarChar, 30);
        addAttribute(recordDescriptor, "Filename", PeterDB::TypeVarChar, 30);
    }

    void createAttrRecordDescriptor(std::vector<PeterDB::Attribute> &recordDescriptor) {
        addAttribute(recordDescriptor, "Table_id", PeterDB::TypeInt, 4);
        addAttribute(recordDescriptor, "Attribute_name", PeterDB::TypeVarChar, 30);
        addAttribute(recordDescriptor, "Type", PeterDB::TypeInt, 4);
        addAttribute(recordDescriptor, "Position", PeterDB::TypeInt, 4);
    }

    static unsigned char *initializeNullFieldsIndicator(const std::vector<PeterDB::Attribute> &recordDescriptor) {
        int nullFieldsIndicatorActualSize = ceil((double) recordDescriptor.size() / CHAR_BIT);
        auto indicator = new unsigned char[nullFieldsIndicatorActualSize];
        memset(indicator, 0, nullFieldsIndicatorActualSize);
        return indicator;
    }


    static void prepareTablesRecord(size_t fieldCount,
                                    unsigned char *nullFieldsIndicator,
                                    const int id,
                                    const int nameLength,
                                    const std::string &name,
                                    const int fileNameLength,
                                    const std::string &fileName,
                                    void *buffer,
                                    size_t &recordSize) {
        int offset = 0;

        // Null-indicators
        int nullFieldsIndicatorActualSize = ceil((double) fieldCount / CHAR_BIT);

        // Null-indicator for the fields
        memcpy((char *) buffer + offset, nullFieldsIndicator,
               nullFieldsIndicatorActualSize);
        offset += nullFieldsIndicatorActualSize;

        // id field
        memcpy((char *) buffer + offset, &id, sizeof(int));
        offset += sizeof(int);

        // name field
        memcpy((char *) buffer + offset, &nameLength, sizeof(int));
        offset += sizeof(int);
        memcpy((char *) buffer + offset, name.c_str(), nameLength);
        offset += nameLength;

        // filename field
        memcpy((char *) buffer + offset, &fileNameLength, sizeof(int));
        offset += sizeof(int);
        memcpy((char *) buffer + offset, fileName.c_str(), fileNameLength);
        offset += fileNameLength;

        recordSize = offset;
    }

    static void prepareAttrsRecord(size_t fieldCount,
                                   unsigned char *nullFieldsIndicator,
                                   const int tableId,
                                   const int attrNameLength,
                                   const std::string &attrName,
                                   const int attrType,
                                   const int position,
                                   void *buffer,
                                   size_t &recordSize) {
        int offset = 0;
        //std::cout << attrName << " " << attrType << std::endl;
        // Null-indicators
        int nullFieldsIndicatorActualSize = ceil((double) fieldCount / CHAR_BIT);

        // Null-indicator for the fields
        memcpy((char *) buffer + offset, nullFieldsIndicator,
               nullFieldsIndicatorActualSize);
        offset += nullFieldsIndicatorActualSize;

        // table id field
        memcpy((char *) buffer + offset, &tableId, sizeof(int));
        offset += sizeof(int);

        // attribute name field
        memcpy((char *) buffer + offset, &attrNameLength, sizeof(int));
        offset += sizeof(int);
        memcpy((char *) buffer + offset, attrName.c_str(), attrNameLength);
        offset += attrNameLength;

        // attribute type field
        memcpy((char *) buffer + offset, &attrType, sizeof(int));
        //std::cout << "After adding type " << *(int*)((char*)buffer + offset) << std::endl;
        offset += sizeof(int);

        // position field
        memcpy((char *) buffer + offset, &position, sizeof(int));
        offset += sizeof(int);

        recordSize = offset;
    }

    RC RelationManager::createCatalog() {

//        create files and record descriptors for Tables table and Attributes table
        RecordBasedFileManager::instance().createFile(tablesFileName);
        RecordBasedFileManager::instance().openFile(tablesFileName, tablesFileHandle);
        createTableRecordDescriptor(tablesRecordDescriptor);

        RecordBasedFileManager::instance().createFile(attrsFileName);
        RecordBasedFileManager::instance().openFile(attrsFileName, attrsFileHandle);
        createAttrRecordDescriptor(attrsRecordDescriptor);

        /********************
        * Tables table *
        *********************/
//         prepare records
        unsigned char *nullsIndicator = initializeNullFieldsIndicator(tablesRecordDescriptor);
        size_t tableRecordSize = 0;
        size_t attrRecordSize = 0;
        void* tablesBuffer = malloc(100);
        void* attrsBuffer = malloc(100);
        prepareTablesRecord(tablesRecordDescriptor.size(), nullsIndicator, 0, 6,
                            "Tables", 6, tablesFileName, tablesBuffer, tableRecordSize);
        prepareTablesRecord(tablesRecordDescriptor.size(), nullsIndicator, 1, 7,
                            "Columns", 7, attrsFileName, attrsBuffer, attrRecordSize);

//         insert records
        RID tablesRid;
        RecordBasedFileManager::instance().insertRecord(tablesFileHandle, tablesRecordDescriptor, tablesBuffer, tablesRid);
        RID attrsRid;
        RecordBasedFileManager::instance().insertRecord(tablesFileHandle, tablesRecordDescriptor, attrsBuffer, attrsRid);
        FileHandle fileHandle = tablesFileHandle;

        free(tablesBuffer);
        free(attrsBuffer);

        /********************
        * Attributes table *
        *********************/
        // prepare Tables records
        void* idRecBuf = malloc(100);
        prepareAttrsRecord(attrsRecordDescriptor.size(), nullsIndicator, 0, 2, "Id", PeterDB::TypeInt, 0, idRecBuf,
                           tableRecordSize);
        void* nameRecBuf = malloc(100);
        prepareAttrsRecord(attrsRecordDescriptor.size(), nullsIndicator, 0, 4, "Name", PeterDB::TypeVarChar, 1, nameRecBuf,
                           tableRecordSize);
        void* filenameRecBuf = malloc(100);
        prepareAttrsRecord(attrsRecordDescriptor.size(), nullsIndicator, 0, 8, "Filename", PeterDB::TypeVarChar, 2, filenameRecBuf,
                           tableRecordSize);
        //std::cout << "After preparing " << *((char*)filenameRecBuf + 16) << std::endl;


//         insert Tables records into Attributes table
//        std::cout << "Inserting tables records " << std::endl;
        RID idRid;
        RecordBasedFileManager::instance().insertRecord(attrsFileHandle, attrsRecordDescriptor, idRecBuf, idRid);
        RID nameRid;
        RecordBasedFileManager::instance().insertRecord(attrsFileHandle, attrsRecordDescriptor, nameRecBuf, nameRid);
        RID filenameRid;
        RecordBasedFileManager::instance().insertRecord(attrsFileHandle, attrsRecordDescriptor, filenameRecBuf, filenameRid);

        free(idRecBuf);
        free(nameRecBuf);
        free(filenameRecBuf);

//         prepare Attributes records
        void* tableIdRecBuf = malloc(100);
//        std::cout << "Inserting attributes records " << std::endl;
        prepareAttrsRecord(attrsRecordDescriptor.size(), nullsIndicator, 1, 8,"Table_id", PeterDB::TypeInt, 0, tableIdRecBuf,
                           tableRecordSize);
        void* attrNameRecBuf = malloc(100);
        prepareAttrsRecord(tablesRecordDescriptor.size(), nullsIndicator, 1, 14, "Attribute_name", PeterDB::TypeVarChar, 1, attrNameRecBuf,
                           tableRecordSize);
        void* attrTypeRecBuf = malloc(100);
        prepareAttrsRecord(tablesRecordDescriptor.size(), nullsIndicator, 1, 4, "Type", PeterDB::TypeInt, 2, attrTypeRecBuf,
                           tableRecordSize);
        void* positionRecBuf = malloc(100);
        prepareAttrsRecord(tablesRecordDescriptor.size(), nullsIndicator, 1, 8, "Position", PeterDB::TypeInt, 3, positionRecBuf,
                           tableRecordSize);

        // insert Attributes records into Attributes table
        RID tableIdRid;
        RecordBasedFileManager::instance().insertRecord(attrsFileHandle, attrsRecordDescriptor, tableIdRecBuf, tablesRid);
        RID attrNameRid;
        RecordBasedFileManager::instance().insertRecord(attrsFileHandle, attrsRecordDescriptor, attrNameRecBuf, attrNameRid);
        RID attrTypeRid;
        RecordBasedFileManager::instance().insertRecord(attrsFileHandle, attrsRecordDescriptor, attrTypeRecBuf, attrTypeRid);
        RID posRid;
        RecordBasedFileManager::instance().insertRecord(attrsFileHandle, attrsRecordDescriptor, positionRecBuf, posRid);

        free(tableIdRecBuf);
        free(attrNameRecBuf);
        free(attrTypeRecBuf);
        free(positionRecBuf);
        RecordBasedFileManager::instance().closeFile(tablesFileHandle);
        RecordBasedFileManager::instance().closeFile(attrsFileHandle);
        return 0;
    }

    // Check whether the given file exists
    static bool fileExists(const std::string &fileName) {
        struct stat stFileInfo{};

        return stat(fileName.c_str(), &stFileInfo) == 0;
    }

    RC RelationManager::deleteCatalog() {
        // check if Table table exists
        if(fileExists(tablesFileName)){
            // delete Tables file
            // delete Attributes file
            RecordBasedFileManager::instance().destroyFile(tablesFileName);
            RecordBasedFileManager::instance().destroyFile(attrsFileName);

            // delete Attribute and Table entries from Tables table
//            RID tablesRid;
//            tablesRid.pageNum = 0;
//            tablesRid.slotNum = 0;
//            RecordBasedFileManager::instance().deleteRecord(tablesFileHandle, tablesRecordDescriptor, tablesRid);
//
//            RID attrsRid;
//            attrsRid.pageNum = 0;
//            attrsRid.slotNum = 1;
//            RecordBasedFileManager::instance().deleteRecord(attrsFileHandle, attrsRecordDescriptor, attrsRid);
        }

        else{
            std::cout<<"Catalog doesn't exist"<<std::endl;
            return -1;
        }

        return 0;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        return -1;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        return -1;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {

        std::string conditionAttribute = "Name";
        PeterDB::CompOp compOp = PeterDB::EQ_OP;
        std::vector<std::string> attributeNames {"Id"};
        RM_ScanIterator rmScanIterator_table, rmScanIterator_attribute;
        int id;
        RID rid;

        //get the id of the table name
        void *value = malloc (tableName.length() + sizeof (int));
        int tableNameLength = tableName.length();
        memcpy(value, &tableNameLength, sizeof (int));
        memcpy((char*)value + sizeof (int), tableName.c_str(), tableName.length());
        scan("Tables", conditionAttribute, compOp, value, attributeNames, rmScanIterator_table);
        free(value);

        void *outBuffer = malloc(1000);
        rmScanIterator_table.getNextTuple(rid, outBuffer);
        memcpy(&id, (char*)outBuffer + 1, sizeof (int));

        //get the attributes from the columns table using the id
        conditionAttribute = "Table_id";
        attributeNames = {"Attribute_name", "Type"};
        value = malloc (sizeof (int));
        memcpy(value, &id, sizeof (int));
        scan("Columns", conditionAttribute, compOp, value, attributeNames, rmScanIterator_attribute);

        memset(outBuffer, 0, 1000);
        while (rmScanIterator_attribute.getNextTuple(rid, outBuffer) != RBFM_EOF) {
            Attribute newAttr;
            int length = *(int*)((char*)outBuffer + 1);

            std::string name ((char*)outBuffer + 1 + sizeof (int), length);
            newAttr.name = name;
//            std::cout << "Last letter  " << *((char*)outBuffer + 1 + sizeof (int) + length - 1) << std::endl;
            int type = *(int*) ((char*)outBuffer + 1 + sizeof (int) + length);

            newAttr.type = static_cast<PeterDB::AttrType>(type);
            //std::cout << "In setting " << type << std::endl;

            newAttr.length = (type == PeterDB::TypeVarChar)? 30: 4;
            attrs.push_back(newAttr);
            memset(outBuffer, 0, 1000);
        }

//        std::cout << "ID of " << tableName << " is " << id << std::endl;
        free(outBuffer);
        return 0;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        return -1;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        return -1;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        return -1;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        return -1;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        return -1;
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        return -1;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {

        PeterDB::RecordBasedFileManager &rbfm = PeterDB::RecordBasedFileManager::instance();
        FileHandle fileHandle;
        std::vector<Attribute> recordDescriptor;

        if (tableName == "Tables") {
            createTableRecordDescriptor(recordDescriptor);
            rbfm.openFile("Tables", fileHandle);
        }
        else if (tableName == "Columns") {
            createAttrRecordDescriptor(recordDescriptor);
            rbfm.openFile("Columns", fileHandle);
        }
        else {
            getAttributes(tableName, recordDescriptor);
        }

        rbfm.scan(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.RBFM_Scan_Iterator);

        return 0;
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
        return RBFM_Scan_Iterator.getNextRecord(rid, data);
    }

    RC RM_ScanIterator::close() { return -1; }

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        return -1;
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        return -1;
    }

    // QE IX related
    RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName){
        return -1;
    }

    RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName){
        return -1;
    }

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC RelationManager::indexScan(const std::string &tableName,
                                  const std::string &attributeName,
                                  const void *lowKey,
                                  const void *highKey,
                                  bool lowKeyInclusive,
                                  bool highKeyInclusive,
                                  RM_IndexScanIterator &rm_IndexScanIterator){
        return -1;
    }


    RM_IndexScanIterator::RM_IndexScanIterator() = default;

    RM_IndexScanIterator::~RM_IndexScanIterator() = default;

    RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key){
        return -1;
    }

    RC RM_IndexScanIterator::close(){
        return -1;
    }

} // namespace PeterDB