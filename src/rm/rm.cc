#include "src/include/rm.h"
#include <iostream>
#include <cstring>
#include <sys/stat.h>
#include <cmath>

#define CHAR_BIT    8

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

    bool canAccess(std::string tableName){
        if(tableName == tablesFileName || tableName == attrsFileName){
            return false;
        }
        return true;
    }

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
        addAttribute(recordDescriptor, "table-id", PeterDB::TypeInt, 4);
        addAttribute(recordDescriptor, "table-name", PeterDB::TypeVarChar, 50);
        addAttribute(recordDescriptor, "file-name", PeterDB::TypeVarChar, 50);
    }

    void createAttrRecordDescriptor(std::vector<PeterDB::Attribute> &recordDescriptor) {
        addAttribute(recordDescriptor, "table-id", PeterDB::TypeInt, 4);
        addAttribute(recordDescriptor, "column-name", PeterDB::TypeVarChar, 50);
        addAttribute(recordDescriptor, "column-type", PeterDB::TypeInt, 4);
        addAttribute(recordDescriptor, "column-length", PeterDB::TypeInt, 4);
        addAttribute(recordDescriptor, "column-position", PeterDB::TypeInt, 4);
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
                                    void *buffer) {
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
    }

    static void prepareAttrsRecord(size_t fieldCount,
                                   unsigned char *nullFieldsIndicator,
                                   const int tableId,
                                   const int attrNameLength,
                                   const std::string &attrName,
                                   const int attrType,
                                   const int attrLen,
                                   const int position,
                                   void *buffer) {
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

        // attribute length field
        memcpy((char *) buffer + offset, &attrLen, sizeof(int));
        //std::cout << "After adding type " << *(int*)((char*)buffer + offset) << std::endl;
        offset += sizeof(int);

        // position field
        memcpy((char *) buffer + offset, &position, sizeof(int));
        offset += sizeof(int);
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
        void* tablesBuffer = malloc(100);
        void* attrsBuffer = malloc(100);
        prepareTablesRecord(tablesRecordDescriptor.size(), nullsIndicator, 0, tablesFileName.length(),
                            tablesFileName, tablesFileName.length(), tablesFileName, tablesBuffer);
        prepareTablesRecord(tablesRecordDescriptor.size(), nullsIndicator, 1, attrsFileName.length(),
                            attrsFileName, attrsFileName.length(), attrsFileName, attrsBuffer);

//         insert records
        RID tablesRid;
        RecordBasedFileManager::instance().insertRecord(tablesFileHandle, tablesRecordDescriptor, tablesBuffer, tablesRid);
        RID attrsRid;
        RecordBasedFileManager::instance().insertRecord(tablesFileHandle, tablesRecordDescriptor, attrsBuffer, attrsRid);

        free(tablesBuffer);
        free(attrsBuffer);

        /********************
        * Attributes table *
        *********************/
        // prepare Tables records
        void* idRecBuf = malloc(100);
        prepareAttrsRecord(attrsRecordDescriptor.size(), nullsIndicator, 0, 8, "table-id", PeterDB::TypeInt,4, 1, idRecBuf);
        void* nameRecBuf = malloc(100);
        prepareAttrsRecord(attrsRecordDescriptor.size(), nullsIndicator, 0, 10, "table-name", PeterDB::TypeVarChar,50, 2, nameRecBuf);
        void* filenameRecBuf = malloc(100);
        prepareAttrsRecord(attrsRecordDescriptor.size(), nullsIndicator, 0, 9, "file-name", PeterDB::TypeVarChar, 50, 3, filenameRecBuf);
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
        prepareAttrsRecord(attrsRecordDescriptor.size(), nullsIndicator, 1, 8,"table-id", PeterDB::TypeInt, 4, 1, tableIdRecBuf);
        void* attrNameRecBuf = malloc(100);
        prepareAttrsRecord(attrsRecordDescriptor.size(), nullsIndicator, 1, 11, "column-name", PeterDB::TypeVarChar, 50, 2, attrNameRecBuf);
        void* attrTypeRecBuf = malloc(100);
        prepareAttrsRecord(attrsRecordDescriptor.size(), nullsIndicator, 1, 11, "column-type", PeterDB::TypeInt, 4, 3, attrTypeRecBuf);
        void* attrLenRecBuf = malloc(100);
        prepareAttrsRecord(attrsRecordDescriptor.size(), nullsIndicator, 1, 13, "column-length", PeterDB::TypeInt, 4, 4, attrLenRecBuf);
        void* positionRecBuf = malloc(100);
        prepareAttrsRecord(attrsRecordDescriptor.size(), nullsIndicator, 1, 15, "column-position", PeterDB::TypeInt,4, 5, positionRecBuf);

        // insert Attributes records into Attributes table
        RID tableIdRid;
        RecordBasedFileManager::instance().insertRecord(attrsFileHandle, attrsRecordDescriptor, tableIdRecBuf, tablesRid);
        RID attrNameRid;
        RecordBasedFileManager::instance().insertRecord(attrsFileHandle, attrsRecordDescriptor, attrNameRecBuf, attrNameRid);
        RID attrTypeRid;
        RecordBasedFileManager::instance().insertRecord(attrsFileHandle, attrsRecordDescriptor, attrTypeRecBuf, attrTypeRid);
        RID attrLenRid;
        RecordBasedFileManager::instance().insertRecord(attrsFileHandle, attrsRecordDescriptor, attrLenRecBuf, attrLenRid);
        RID posRid;
        RecordBasedFileManager::instance().insertRecord(attrsFileHandle, attrsRecordDescriptor, positionRecBuf, posRid);

        free(tableIdRecBuf);
        free(attrNameRecBuf);
        free(attrTypeRecBuf);
        free(attrLenRecBuf);
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
        }

        else{
            std::cout<<"Catalog doesn't exist"<<std::endl;
            return -1;
        }

        return 0;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        // check if catalog exists
        if(!canAccess(tableName)) {
            return -1;
        }

        if(fileExists(tablesFileName) && fileExists(attrsFileName)){
            if(fileExists(tableName)){
                std::cout<<"Table already exists"<<std::endl;
                return -1;
            }

            //create a file for the table
            RecordBasedFileManager::instance().createFile(tableName);

            unsigned char *nullsIndicator = initializeNullFieldsIndicator(tablesRecordDescriptor);

            // prepare Tables record
            void *outBuffer = malloc(1000);
            RM_ScanIterator rmsi;
            std::vector<std::string> attributeNames {"table-id"};
            scan(tablesFileName,"", PeterDB::NO_OP, nullptr, attributeNames, rmsi);
            memset(outBuffer, 0, 1000);
            RID rid;
            int idCounter = 0;
            while (rmsi.getNextTuple(rid, outBuffer) != RBFM_EOF) {
                idCounter++;
            }
            void* tablesBuffer = malloc(100);
            RID newRid;
            prepareTablesRecord(tablesRecordDescriptor.size(), nullsIndicator, idCounter,
                                tableName.length(), tableName, tableName.length(),
                                tableName, tablesBuffer);
            // insert record into Tables table
            RecordBasedFileManager::instance().openFile(tablesFileName, tablesFileHandle);
            RecordBasedFileManager::instance().insertRecord(tablesFileHandle, tablesRecordDescriptor, tablesBuffer, newRid);
            RecordBasedFileManager::instance().closeFile(tablesFileHandle);
            // prepare Attributes record
            int positionCounter = 0;
            for(auto attr: attrs){
                void* attrBuffer = malloc(1000);
                prepareAttrsRecord(attrsRecordDescriptor.size(), nullsIndicator,
                                   idCounter, attr.name.length(), attr.name, attr.type, attr.length,
                                   positionCounter, attrBuffer);

                RecordBasedFileManager::instance().openFile(attrsFileName, attrsFileHandle);
                RecordBasedFileManager::instance().insertRecord(attrsFileHandle, attrsRecordDescriptor, attrBuffer, newRid);
                RecordBasedFileManager::instance().closeFile(attrsFileHandle);
                positionCounter++;
//                RecordBasedFileManager::instance().printRecord(attrsRecordDescriptor, attrBuffer, std::cout );
            }

//            RecordBasedFileManager::instance().printRecord(tablesRecordDescriptor, tablesBuffer, std::cout );
//            RecordBasedFileManager::instance().printRecord(attrsRecordDescriptor, attrB, std::cout );

        }
        else{
            std::cout<<"Catalog does not exist, cannot create table"<<std::endl;
            return -1;
        }
        return 0;
    }


    RC RelationManager::deleteTable(const std::string &tableName) {
        if(!canAccess(tableName)) {
            return -1;
        }

        // delete file
        // delete record


        //get the id of the table name from Tables
        std::string conditionAttribute = "table-name";
        PeterDB::CompOp compOp = PeterDB::EQ_OP;
        std::vector<std::string> attributeNames{"table-id"};
        RM_ScanIterator rmScanIterator1, rmScanIterator2;
        int id;
        RID rid;

        void *value = malloc(tableName.length() + sizeof(int));
        int tableNameLength = tableName.length();
        memcpy(value, &tableNameLength, sizeof(int));
        memcpy((char *) value + sizeof(int), tableName.c_str(), tableName.length());
        scan(tablesFileName, conditionAttribute, compOp, value, attributeNames, rmScanIterator1);
        free(value);


        void *outBuffer = malloc(1000);
        if (rmScanIterator1.getNextTuple(rid, outBuffer) == RBFM_EOF)
            return -1;

        RecordBasedFileManager::instance().openFile(tablesFileName, tablesFileHandle);
        RecordBasedFileManager::instance().deleteRecord(tablesFileHandle, tablesRecordDescriptor, rid);
        RecordBasedFileManager::instance().closeFile(tablesFileHandle);
        memcpy(&id, (char *) outBuffer + 1, sizeof(int));


        //get the attributes from the columns table using the id
        conditionAttribute = "table-id";
        attributeNames = {"column-name", "column-type"};
        value = malloc(sizeof(int));
        memcpy(value, &id, sizeof(int));
        scan(attrsFileName, conditionAttribute, compOp, value, attributeNames, rmScanIterator2);


        memset(outBuffer, 0, 1000);


        while (rmScanIterator2.getNextTuple(rid, outBuffer) != RBFM_EOF) {
            RecordBasedFileManager::instance().openFile(attrsFileName, attrsFileHandle);
            RecordBasedFileManager::instance().deleteRecord(attrsFileHandle, attrsRecordDescriptor, rid);
            RecordBasedFileManager::instance().closeFile(attrsFileHandle);
            memset(outBuffer, 0, 1000);
        }
        RecordBasedFileManager::instance().destroyFile(tableName);
        return 0;
    }


    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {

        std::string conditionAttribute = "table-name";
        PeterDB::CompOp compOp = PeterDB::EQ_OP;
        std::vector<std::string> attributeNames {"table-id"};
        RM_ScanIterator rmScanIterator_table, rmScanIterator_attribute;
        int id;
        RID rid;

        //get the id of the table name
        void *value = malloc (tableName.length() + sizeof (int));
        int tableNameLength = tableName.length();
        memcpy(value, &tableNameLength, sizeof (int));
        memcpy((char*)value + sizeof (int), tableName.c_str(), tableName.length());
        scan(tablesFileName, conditionAttribute, compOp, value, attributeNames, rmScanIterator_table);
        free(value);

        void *outBuffer = malloc(1000);
        if (rmScanIterator_table.getNextTuple(rid, outBuffer) == RBFM_EOF ) {
            free(outBuffer);
            return -1;
        }

        memcpy(&id, (char*)outBuffer + 1, sizeof (int));

        //get the attributes from the columns table using the id
        conditionAttribute = "table-id";
        attributeNames = {"column-name", "column-type", "column-length"};
        value = malloc (sizeof (int));
        memcpy(value, &id, sizeof (int));
        scan(attrsFileName, conditionAttribute, compOp, value, attributeNames, rmScanIterator_attribute);

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

            int column_length = *(int*) ((char*)outBuffer + 1 + sizeof (int) + length + sizeof (int));
            newAttr.length = column_length;
            // std:: cout << "Column size " << column_length << std::endl;
            attrs.push_back(newAttr);
            memset(outBuffer, 0, 1000);
        }

//        std::cout << "ID of " << tableName << " is " << id << std::endl;
        free(outBuffer);
        return 0;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        if(!canAccess(tableName)) {
            return -1;
        }

        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

        std::vector<PeterDB::Attribute > attrs;
        FileHandle fileHandle;

        if(tableName == tablesFileName)
            attrs = tablesRecordDescriptor;
        else if (tableName == attrsFileName)
            attrs = attrsRecordDescriptor;
        else
            getAttributes(tableName, attrs);

        if(rbfm.openFile(tableName, fileHandle)) return -1;
        rbfm.insertRecord(fileHandle, attrs, data, rid);
        rbfm.closeFile(fileHandle);
        return 0;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {

        if(!canAccess(tableName)) {
            return -1;
        }
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

        std::vector<PeterDB::Attribute > attrs;
        FileHandle fileHandle;

        if(tableName == tablesFileName)
            attrs = tablesRecordDescriptor;
        else if (tableName == attrsFileName)
            attrs = attrsRecordDescriptor;
        else
            getAttributes(tableName, attrs);

        rbfm.openFile(tableName, fileHandle);
        rbfm.deleteRecord(fileHandle, attrs, rid);
        rbfm.closeFile(fileHandle);
        return 0;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

        std::vector<PeterDB::Attribute > attrs;
        FileHandle fileHandle;

        getAttributes(tableName, attrs);
        rbfm.openFile(tableName, fileHandle);
        rbfm.updateRecord(fileHandle, attrs, data, rid);
        rbfm.closeFile(fileHandle);
        return 0;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

        std::vector<PeterDB::Attribute > attrs;
        FileHandle fileHandle;

         if(tableName == tablesFileName)
            attrs = tablesRecordDescriptor;
        else if (tableName == attrsFileName)
            attrs = attrsRecordDescriptor;
        else
            getAttributes(tableName, attrs);
        if(rbfm.openFile(tableName.c_str(), fileHandle)) return -1;
        if(rbfm.readRecord(fileHandle, attrs, rid, data)) return -1;
        rbfm.closeFile(fileHandle);
        return 0;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

        rbfm.printRecord(attrs, data, out);
        return 0;
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        std::vector<Attribute > recordDescriptor;
        getAttributes(tableName, recordDescriptor);
        FileHandle fileHandle;
        RecordBasedFileManager::instance().openFile(tableName, fileHandle);
        RecordBasedFileManager::instance().readAttribute(fileHandle, recordDescriptor, rid, attributeName, data);
        RecordBasedFileManager::instance().closeFile(fileHandle);
        return 0;
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

        if (tableName == tablesFileName) {
            createTableRecordDescriptor(recordDescriptor);
            rbfm.openFile(tablesFileName, fileHandle);
        }
        else if (tableName == attrsFileName) {
            createAttrRecordDescriptor(recordDescriptor);
            rbfm.openFile(attrsFileName, fileHandle);
        }
        else {
            if (getAttributes(tableName, recordDescriptor))
                return -1;
            rbfm.openFile(tableName, fileHandle);
        }

        rbfm.scan(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.RBFM_Scan_Iterator);

        return 0;
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
        int retVal = RBFM_Scan_Iterator.getNextRecord(rid, data);
        if(retVal){
            return RM_EOF;
        }
        return 0;
    }

    RC RM_ScanIterator::close() { return 0; }

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