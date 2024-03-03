#include <cassert>
#include <cstring>
#include <algorithm>
#include "src/include/ix.h"

typedef unsigned    NumKeysLength;
typedef unsigned    FreeSpaceLength;
typedef unsigned    SiblingPtrLength;
typedef char        isLeafIndicator;

#define FREE_SPACE_BYTES    sizeof(FreeSpaceLength)
#define NUM_KEYS_BYTES      sizeof(NumKeysLength)
#define IS_LEAF_BYTES       sizeof(isLeafIndicator)
#define SIBLING_BYTES       sizeof(SiblingPtrLength)

#define FREE_SPACE_OFFSET   (PAGE_SIZE - FREE_SPACE_BYTES - 1)
#define NUM_KEYS_OFFSET     (FREE_SPACE_OFFSET - NUM_KEYS_BYTES)
#define IS_LEAF_OFFSET      (NUM_KEYS_OFFSET - IS_LEAF_BYTES)
#define SIB_PTR             (IS_LEAF_OFFSET - SIBLING_BYTES)

namespace PeterDB {
    IndexManager &IndexManager::instance() {
        static IndexManager _index_manager = IndexManager();
        return _index_manager;
    }

    RC IXFileHandle::iXReadPage(PageNum pageNum, void *data) {
        if(fileHandle.readPage(pageNum, data)){
            return -1;
        }
        ixReadPageCounter++;
        return 0;
    }
    RC IXFileHandle::iXWritePage(PageNum pageNum, const void *data) {
        if(fileHandle.writePage(pageNum, data)) {
            return -1;
        }
        ixWritePageCounter++;
        return 0;
    }

    RC IXFileHandle::iXAppendPage(const void *data) {
        if (fileHandle.appendPage(data)) {
            return -1;
        }
        ixAppendPageCounter++;
        return 0;
    }

    unsigned IXFileHandle::iXgetNumberOfPages() {
        return ixAppendPageCounter;
    }

    LeafNode::LeafNode(){
        next = -1;
        numKeys = 0;
        freeSpace = PAGE_SIZE - FREE_SPACE_BYTES - NUM_KEYS_BYTES - IS_LEAF_BYTES - SIBLING_BYTES;
        isLeaf = 1;
    }

    InternalNode::InternalNode() {
        numKeys = 0;
        freeSpace = PAGE_SIZE - FREE_SPACE_BYTES - NUM_KEYS_BYTES - IS_LEAF_BYTES;
        isLeaf = 0;
    }

    void readDummyHeadFromFile (IXFileHandle &iXFileHandle) {
        char hiddenPage[PAGE_SIZE];
        iXFileHandle.iXReadPage(0, hiddenPage);
        memcpy(&iXFileHandle.dummyHead, hiddenPage,  sizeof(PageNum));
    }

    void writeDummyHeadToFile (IXFileHandle &iXFileHandle) {
        char hiddenPage[PAGE_SIZE];
        memcpy(hiddenPage, &iXFileHandle.dummyHead, sizeof(PageNum));
        iXFileHandle.iXWritePage(0, hiddenPage);
    }

    template<typename K>
    int findChildIndex(const std::vector<K>& keys, const K targetKey) {
        // This function finds the correct child index based on the target key
        int index = 0; // Default to the first child
        while (index < keys.size() && targetKey >= keys[index]) {
            index++;
        }
        return index;
    }

    template<>
    int findChildIndex<char*>(const std::vector<char*>& keys, char* targetKey){
        int index = 0;
        while (index < keys.size()) {
            int keyLen = *(int*)keys[index];
            std::string keyStr(keys[index]+sizeof(int), keyLen);
            int targetKeyLen = *(int*)targetKey;
            std::string targetKeyStr(targetKey+sizeof(int), targetKeyLen);
            if(targetKeyStr < keyStr){
                break;
            }
            index++;
        }
        return index;
    }

    int findChildPointer (InternalNode &internalNode, const void* key, const Attribute &attribute) {
        int returnedPage = -1;
        switch(attribute.type){
            case TypeInt:
                returnedPage = findChildIndex<int>(internalNode.intKeys, *(int*)key);
                break;
            case TypeReal:
                returnedPage = findChildIndex<float>(internalNode.floatKeys, *(float*)key);
                break;
            case TypeVarChar:
                returnedPage = findChildIndex<char*>(internalNode.varcharKeys, (char*)key);
                break;
        }
        return internalNode.children[returnedPage];
    }

    void createBPlusTree(IXFileHandle &iXFileHandle, LeafNode &leafNode, const Attribute &attribute) {
        iXFileHandle.dummyHead = 1;
        if(iXFileHandle.fileHandle.getNumberOfPages() == 0){
            //create hidden page
            char hiddenPage[PAGE_SIZE];
            memcpy(hiddenPage, &iXFileHandle.dummyHead, sizeof(PageNum));
            iXFileHandle.iXAppendPage(hiddenPage);
        }
        leafNode.isLeaf = 1;
        void* sePage[PAGE_SIZE];
        memset(sePage, 0, PAGE_SIZE);
        leafNode.Serialize(sePage, attribute);
        iXFileHandle.iXAppendPage(sePage);
    }

    RC getLeafEntrySize (Attribute attribute, int &size, char *key) {
        size = sizeof (unsigned) + sizeof(short);

        if (attribute.type == TypeVarChar)
            size += sizeof(int) + *(int*)key;
        else
            size += sizeof (int);

        return 0;
    }

    RC getInternalEntrySize (Attribute attribute, int &size, char *key) {
        size = sizeof (PageNum );

        if (attribute.type == TypeVarChar)
            size += sizeof(int) + *(int*)key;
        else
            size += sizeof (int);

        return 0;
    }

    template<typename KeyType>
    bool compareFunction (const std::tuple<KeyType, RID>& lhs, const std::tuple<KeyType, RID>& rhs) {

        if (std::get<0>(lhs) != std::get<0>(rhs)) {

            return std::get<0>(lhs) < std::get<0>(rhs);
        }

        else if (std::get<1>(lhs).pageNum != std::get<1>(rhs).pageNum) {

            return std::get<1>(lhs).pageNum < std::get<1>(rhs).pageNum;
        } else {

            return std::get<1>(lhs).slotNum < std::get<1>(rhs).slotNum;
        }
    }

    template<>
    bool compareFunction<char*> (const std::tuple<char*, RID>& lhs, const std::tuple<char*, RID>& rhs) {

        char *key1 = std::get<0>(lhs);
        char *key2 = std::get<0>(rhs);
        int keyLen1 = *(int*)key1;
        int keyLen2 = *(int*)key2;

        std::string keyStr1(key1 + sizeof (int), keyLen1);
        std::string keyStr2(key2 + sizeof (int), keyLen2);

        //std::cout << "Strings to be compared " << keyStr1 << " " << keyStr2 << std::endl;
        if (keyStr1 != keyStr2) {

            return (keyStr1 < keyStr2);
        }

        else if (std::get<1>(lhs).pageNum != std::get<1>(rhs).pageNum) {

            return std::get<1>(lhs).pageNum < std::get<1>(rhs).pageNum;
        } else {

            return std::get<1>(lhs).slotNum < std::get<1>(rhs).slotNum;
        }
    }

    template<typename KeyType>
    void sortKeys (std::vector<KeyType> &Keys, std::vector<PeterDB::RID>&rids) {
        std::vector<std::tuple<KeyType, PeterDB::RID>> combined;
        for (int i = 0; i < Keys.size(); i++) {
            combined.push_back(std::make_tuple(Keys[i], rids[i]));
        }
        sort(combined.begin(), combined.end(), compareFunction<KeyType>);

        Keys.clear();
        rids.clear();

        for (auto& tuple : combined) {
            Keys.push_back(std::get<0>(tuple));
            rids.push_back(std::get<1>(tuple));
        }
    }

    int findKey (Node* node, const void *searchKey, const Attribute &attribute, CompOp compOp) {
        if(attribute.type == PeterDB::TypeInt){
            std::cout << "Finding Key " << *(int*)searchKey << std::endl;
            for (int i = 0; i < node->intKeys.size(); i++) {
                if (compOp == GE_OP) {
                    if(node->intKeys[i] >= *(int*)searchKey) {
                        return i;
                    }
                } else {
                    if(node->intKeys[i] > *(int*)searchKey) {
                        return i;
                    }
                }
            }
        }
        else if(attribute.type == PeterDB::TypeReal){
            for (int i = 0; i < node->floatKeys.size(); i++) {
                if (compOp == GE_OP) {
                    if(node->floatKeys[i] >= *(float*)searchKey) {
                        return i;
                    }
                } else {
                    if(node->floatKeys[i] > *(float*)searchKey) {
                        return i;
                    }
                }
            }
        }
        else{
            for (int i = 0; i < node->varcharKeys.size(); i++) {
                int keyLen = *(int*)node->varcharKeys[i];
                char keyVal[keyLen];
                memcpy(keyVal, node->varcharKeys[i] + sizeof(keyLen), keyLen);
//                std::cout << "Keys already present : " << keyVal[0] << " " << keyVal[keyLen-1] << std::endl;
//                std::cout << "Searching key : " << *((char*)searchKey + 4) << " " << *((char*)searchKey + 4 + *(int*)searchKey) << std::endl;
//                std::cout << "While finding " << keyLen << std::endl;
                if (compOp == GE_OP) {
                    if (memcmp(node->varcharKeys[i], searchKey, keyLen + sizeof (keyLen)) >= 0)
                        return i;
                } else {
                    if (memcmp(node->varcharKeys[i], searchKey, keyLen + sizeof (keyLen)) > 0)
                        return i;
                }
            }
        }
        return -1;
    }


    void pushKeyToLeafVector (const Attribute &attribute, const void *key, LeafNode &leafNode) {
        // insert into appropriate index (will include checking for RIDs if keys are duplicate)
        int sizeOffset;
        switch (attribute.type) {
            case TypeInt:
                leafNode.intKeys.push_back(*(int *) key);
                sortKeys<int>(leafNode.intKeys, leafNode.rids);
                sizeOffset = sizeof(int);
                break;
            case TypeReal:
                leafNode.floatKeys.push_back(*(float *) key);
                sortKeys<float>(leafNode.floatKeys, leafNode.rids);
                sizeOffset = sizeof(float);
                break;
            case TypeVarChar:
                leafNode.varcharKeys.push_back((char *) key);
                sortKeys<char*>(leafNode.varcharKeys, leafNode.rids);
                int keyLen = *(int *) key; // Length of VarChar key
                sizeOffset = sizeof(int) + keyLen; // Additional size for VarChar length
                break;
        }

        // Update free space for all types
        leafNode.freeSpace -= (sizeOffset + sizeof(unsigned) + sizeof(short));
        leafNode.numKeys++;
    }

    int insertBeforeFirstLarger(InternalNode &internalNode, const void* key, const Attribute &attribute) {
        int index = findKey(&internalNode, key, attribute, GE_OP);
        switch (attribute.type) {
            case TypeReal:
                if(index == -1){
                    internalNode.floatKeys.push_back(*(float*)key);
                    return internalNode.floatKeys.size()-1;
                }
                internalNode.floatKeys.insert(internalNode.floatKeys.begin()+index, *(float*)key);
                return index;
            case TypeInt:
                if(index == -1){
                    internalNode.intKeys.push_back(*(int*)key);
                    return internalNode.floatKeys.size()-1;
                }
                internalNode.intKeys.insert(internalNode.intKeys.begin()+index, *(int*)key);
                return index;

            case TypeVarChar:
                if(index == -1){
                    internalNode.varcharKeys.push_back((char*)key);
                    return internalNode.varcharKeys.size()-1;
                }
                internalNode.varcharKeys.insert(internalNode.varcharKeys.begin()+index, (char*)key);
                return index;

        }
    }

    void copyKeyToInternalVector (const Attribute &attribute, const void *smallestKey, InternalNode &internalNode,
                                  PageNum leftPtr, PageNum rightPtr) {
        int sizeOffset;
        int index = -1;
        index = insertBeforeFirstLarger(internalNode, smallestKey, attribute);
        switch (attribute.type) {
            case TypeInt:
                sizeOffset = sizeof(int);
                break;
            case TypeReal:
                sizeOffset = sizeof(float);
                break;
            case TypeVarChar:
                int keyLen = *(int *) smallestKey; // Length of VarChar smallestKey
                sizeOffset = sizeof(int) + keyLen; // Additional size for VarChar length
                break;
        }
        if (leftPtr != -1) {
            // new root
            internalNode.children.push_back(leftPtr);
            internalNode.freeSpace -= (sizeof (PageNum));
        }

        internalNode.freeSpace -= (sizeOffset + (sizeof (PageNum)));

        //add the pointers
        internalNode.children.insert(std::begin(internalNode.children) + index+1, rightPtr);
        internalNode.numKeys++;
    }

    void calculateSpaceLeafNode (LeafNode &leafNode, int &size, const Attribute &attribute) {
        size = 0;
        switch (attribute.type) {
            case TypeVarChar:
                for (auto & varcharKey : leafNode.varcharKeys) {
                    int keyLen = *(int*)varcharKey;
                    size += keyLen + sizeof (keyLen);
                }
                break;
            case TypeInt:
                size = leafNode.intKeys.size() * sizeof(int);
                break;
            case TypeReal:
                size = leafNode.floatKeys.size() * sizeof(float);
                break;
        }
        size += leafNode.rids.size() * (sizeof(int) + sizeof(short int));
    }

    void calculateSpaceInternalNode (InternalNode &internalNode, int &size, const Attribute &attribute) {
        size = 0;
        switch (attribute.type) {
            case TypeVarChar:
                for (auto & varcharKey : internalNode.varcharKeys) {
                    int keyLen = *(int*)varcharKey;
                    size += keyLen + sizeof(keyLen);
                }
                break;
            case TypeInt:
                size += internalNode.intKeys.size() * sizeof(int);
                break;
            case TypeReal:
                size += internalNode.floatKeys.size() * sizeof(float);
                break;
        }
        size += internalNode.children.size() * sizeof(PageNum);

    }

    void updateFreeSpaceSplitLeafNode (LeafNode &leafNode1, LeafNode &leafNode2, const Attribute &attribute) {
        int freeSpace;
        calculateSpaceLeafNode(leafNode2, freeSpace, attribute);
        leafNode1.freeSpace += freeSpace;
        leafNode2.freeSpace -= freeSpace;
    }

    void updateFreeSpaceSplitInternalNode (InternalNode &internalNode1, InternalNode &internalNode2, const Attribute &attribute) {
        int freeSpace;
        calculateSpaceInternalNode(internalNode2, freeSpace, attribute);
        internalNode1.freeSpace += freeSpace;
        internalNode2.freeSpace -= freeSpace;
    }

    int splitInternalNode (InternalNode &node1, InternalNode &node2,
                         IXFileHandle &iXFileHandle, const Attribute& attribute) {
        PageNum newInternalNodePage = iXFileHandle.iXgetNumberOfPages();
        //split keys
        unsigned numKeys = node1.numKeys;
        switch (attribute.type) {
            case TypeInt:
                node2.intKeys = std::vector<int>(node1.intKeys.begin() + (numKeys/2),
                                                     node1.intKeys.end());

                // Erase the last 5 entries from the original vector
                node1.intKeys.erase(node1.intKeys.begin() + (numKeys/2),
                                    node1.intKeys.end());
                node1.numKeys = node1.intKeys.size();
                node2.numKeys = node2.intKeys.size();
                break;

            case TypeReal:
                node2.floatKeys = std::vector<float>(node1.floatKeys.begin() + (numKeys/2),
                                                     node1.floatKeys.end());

                // Erase the last 5 entries from the original vector
                node1.floatKeys.erase(node1.floatKeys.begin() + (numKeys/2),
                                      node1.floatKeys.end());
                node1.numKeys = node1.floatKeys.size();
                node2.numKeys = node2.floatKeys.size();
                break;

            case TypeVarChar:
                node2.varcharKeys = std::vector<char*>(node1.varcharKeys.begin() + (numKeys/2),
                                                       node1.varcharKeys.end());

                // Erase the last 5 entries from the original vector
                node1.varcharKeys.erase(node1.varcharKeys.begin() + (numKeys/2),
                                        node1.varcharKeys.end());
                node1.numKeys = node1.varcharKeys.size();
                node2.numKeys = node2.varcharKeys.size();
                break;
        }

        // split children
        node2.children = std::vector<PageNum >(node1.children.begin() + node1.numKeys + 1,
                                          node1.children.end());
        node1.children.erase(node1.children.begin() + node1.numKeys + 1 ,
                             node1.children.end());

        updateFreeSpaceSplitInternalNode(node1, node2, attribute);
        return newInternalNodePage;
    }

    void splitLeafNodes (LeafNode &leafNode1, LeafNode &leafNode2,
                         IXFileHandle &iXFileHandle, const Attribute& attribute) {
        PageNum newLeafNodePage = iXFileHandle.iXgetNumberOfPages();
        leafNode1.next = newLeafNodePage;
        int numKeys = leafNode1.numKeys;

        switch (attribute.type) {
            case TypeInt:
                leafNode2.intKeys = std::vector<int>(leafNode1.intKeys.end() - (numKeys/2),
                                                     leafNode1.intKeys.end());

                // Erase the last 5 entries from the original vector
                leafNode1.intKeys.erase(leafNode1.intKeys.end() - (numKeys/2),
                                        leafNode1.intKeys.end());
                leafNode1.numKeys = leafNode1.intKeys.size();
                leafNode2.numKeys = leafNode2.intKeys.size();
                break;

            case TypeReal:
                leafNode2.floatKeys = std::vector<float>(leafNode1.floatKeys.end() - (numKeys/2),
                                                         leafNode1.floatKeys.end());

                // Erase the last 5 entries from the original vector
                leafNode1.floatKeys.erase(leafNode1.floatKeys.end() - (numKeys/2),
                                          leafNode1.floatKeys.end());
                leafNode1.numKeys = leafNode1.floatKeys.size();
                leafNode2.numKeys = leafNode2.floatKeys.size();
                break;

            case TypeVarChar:
                leafNode2.varcharKeys = std::vector<char*>(leafNode1.varcharKeys.begin() + (numKeys/2),
                                                           leafNode1.varcharKeys.end());

                // Erase the last 5 entries from the original vector
                leafNode1.varcharKeys.erase(leafNode1.varcharKeys.begin() + (numKeys/2),
                                            leafNode1.varcharKeys.end());
                leafNode1.numKeys = leafNode1.varcharKeys.size();
                leafNode2.numKeys = leafNode2.varcharKeys.size();
                break;
        }
        leafNode2.rids = std::vector<RID>(leafNode1.rids.begin() + (numKeys/2),
                                          leafNode1.rids.end());
        leafNode1.rids.erase(leafNode1.rids.begin() + (numKeys/2),
                             leafNode1.rids.end());
        updateFreeSpaceSplitLeafNode(leafNode1, leafNode2, attribute);
    }


    void Node::writeNodeToFile(Attribute attribute, PageNum pageNum, IXFileHandle &ixFileHandle) {
        char data[PAGE_SIZE];
        memset(data, 0, PAGE_SIZE);
        this->Serialize(data, attribute);
        ixFileHandle.iXWritePage(pageNum, data);
    }

    void Node::appendNodeToFile(PeterDB::Attribute attribute, PeterDB::IXFileHandle &ixFileHandle) {
        char data[PAGE_SIZE];
        memset(data, 0, PAGE_SIZE);
        this->Serialize(data, attribute);
        ixFileHandle.iXAppendPage(data);


    }

    void* getSmallestKey (Node *node, const Attribute &attribute) {
        void *smallestKey;
        switch (attribute.type) {
            case TypeReal:
                smallestKey = malloc(sizeof (float));
                memcpy(smallestKey, &node->floatKeys[0], sizeof (float));
                if(!node->isLeaf){
                    node->floatKeys.erase(node->floatKeys.begin());
                    node->numKeys--;
                }
                break;
            case TypeInt:
                smallestKey = malloc(sizeof (int));
                memcpy(smallestKey, &node->intKeys[0], sizeof (int));
                if(!node->isLeaf){
                    node->intKeys.erase(node->intKeys.begin());
                    node->numKeys--;
                }
                break;
            case TypeVarChar:
                int keyLen = *(int*)node->varcharKeys[0];
                smallestKey = malloc(keyLen + sizeof(keyLen));
                memcpy(smallestKey, node->varcharKeys[0], keyLen+sizeof(int));
                if(!node->isLeaf){
                    node->varcharKeys.erase(node->varcharKeys.begin());
                    node->numKeys--;
                }
                break;
        }
        return smallestKey;
    }

    void insert(PageNum nodePtr, PageNum &newChildEntry, void *&smallestKey,
                const void *key, const RID &rid, IXFileHandle &iXFileHandle,
                const Attribute &attribute) {
        // Load desPage of root
        char desPage[PAGE_SIZE];
        char sePage[PAGE_SIZE];
        memset(sePage, 0, PAGE_SIZE);
        memset(desPage, 0, PAGE_SIZE);

        iXFileHandle.iXReadPage(nodePtr, desPage);
        char isLeaf = *(desPage + IS_LEAF_OFFSET);

        if (isLeaf) {
            LeafNode leafNode;
            leafNode.Deserialize(desPage, attribute);

            int LeafEntryLength;
            getLeafEntrySize(attribute, LeafEntryLength, (char*)key);

            if (leafNode.freeSpace >= LeafEntryLength) {
                // if a leaf has space, we don't care if it's a root or not
                leafNode.rids.push_back(rid);
//                leafNode.numKeys += 1;

                pushKeyToLeafVector (attribute, key, leafNode);

                // Serialize and write back
                leafNode.Serialize(sePage, attribute);
                iXFileHandle.iXWritePage(nodePtr, sePage);
                newChildEntry = -1;
                smallestKey = nullptr;
            }
            else {
                LeafNode newLeafNode;

                leafNode.rids.push_back(rid);
                pushKeyToLeafVector(attribute, key, leafNode);
                splitLeafNodes(leafNode, newLeafNode, iXFileHandle, attribute);

                // write the old leaf and append the new leaf to the file
                leafNode.writeNodeToFile(attribute, nodePtr , iXFileHandle);
                newLeafNode.appendNodeToFile(attribute, iXFileHandle);

                newChildEntry = leafNode.next;
                smallestKey = getSmallestKey(&newLeafNode, attribute);
                int keyLen = *(int*)smallestKey;
                if (nodePtr == iXFileHandle.dummyHead) {
                    //leaf + no space + root
                    InternalNode newRoot;
                    copyKeyToInternalVector(attribute, smallestKey, newRoot,
                                            nodePtr, leafNode.next);
                    iXFileHandle.dummyHead = iXFileHandle.iXgetNumberOfPages();
                    writeDummyHeadToFile(iXFileHandle);
                    //append the new root to the file
                    newRoot.appendNodeToFile(attribute, iXFileHandle);
                    void* newRootPage[PAGE_SIZE];
                    iXFileHandle.iXReadPage(iXFileHandle.iXgetNumberOfPages()-1,newRootPage);
                    newRoot.Deserialize(newRootPage, attribute);
                }
            }
        }
        else {
            //not a leaf
            InternalNode internalNode;
            internalNode.Deserialize(desPage, attribute);
            PageNum returnedPage = -1;
            returnedPage = findChildPointer(internalNode, key, attribute);
            insert(returnedPage, newChildEntry, smallestKey, key, rid, iXFileHandle, attribute);
            if (newChildEntry == -1)
                return;
            else {
                //a leaf has split, add it to the internal node
                int size;
                getInternalEntrySize(attribute, size, (char*)smallestKey);
                if (internalNode.freeSpace >= size) {
                    copyKeyToInternalVector(attribute, smallestKey, internalNode,
                                            -1, newChildEntry);
                    newChildEntry = -1;
                    internalNode.writeNodeToFile(attribute, nodePtr, iXFileHandle);
                }
                else {
                    // internal node + no space
                    InternalNode newInternalNode;
                    copyKeyToInternalVector(attribute, smallestKey, internalNode,
                                            -1, newChildEntry);
                    newChildEntry = splitInternalNode(internalNode, newInternalNode, iXFileHandle, attribute);
                    smallestKey = getSmallestKey(&newInternalNode, attribute);
                    internalNode.writeNodeToFile(attribute, nodePtr, iXFileHandle);
                    newInternalNode.appendNodeToFile(attribute, iXFileHandle);

                    // testing
//                    void* data[PAGE_SIZE];
//                    iXFileHandle.iXReadPage(8, data);
//                    InternalNode testNode;
//                    testNode.Deserialize(data, attribute);
//                    std::cout<<"test node: "<< testNode.isLeaf<<std::endl;

                    if(nodePtr == iXFileHandle.dummyHead){
                        // if the internal node split was a root = creation of new root
                        InternalNode newRoot;
                        PageNum  newRootPageNum = iXFileHandle.iXgetNumberOfPages();
                        copyKeyToInternalVector(attribute, smallestKey, newRoot, nodePtr,
                                                newChildEntry);
                        iXFileHandle.dummyHead = newRootPageNum;
                        writeDummyHeadToFile(iXFileHandle);
                        newRoot.appendNodeToFile(attribute, iXFileHandle);
                    }
                    //testing
//                    void* data[PAGE_SIZE];
//                    iXFileHandle.iXReadPage(8, data);
//                    InternalNode testNode;
//                    testNode.Deserialize(data, attribute);
//                    std::cout<<"test node: "<< testNode.isLeaf<<std::endl;

                }
            }
        }
    }

    int findKeyDelete (LeafNode leafNode, const void *searchKey, const Attribute &attribute, const RID &rid) {
        if(attribute.type == PeterDB::TypeInt){
            std::cout << "Finding Key to delete " << *(int*)searchKey << std::endl;
            for (int i = 0; i < leafNode.intKeys.size(); i++) {
                std::cout << "Going through key " << leafNode.intKeys[i] << " with RID: " << leafNode.rids[i].pageNum
                          << " " << leafNode.rids[i].slotNum << std::endl;

                if(leafNode.intKeys[i] == *(int*)searchKey){
                    if (leafNode.rids[i].pageNum == rid.pageNum)
                        if (leafNode.rids[i].slotNum == rid.slotNum)
                            return i;
                }
            }
        }
        else if(attribute.type == PeterDB::TypeReal){
            for (int i = 0; i < leafNode.floatKeys.size(); i++) {
                if(leafNode.floatKeys[i] == *(float*)searchKey){
                    if (leafNode.rids[i].pageNum == rid.pageNum)
                        if (leafNode.rids[i].slotNum == rid.slotNum)
                            return i;
                }
            }
        }
        else{
            for (int i = 0; i < leafNode.varcharKeys.size(); i++) {
                int keyLen = *(int*)leafNode.varcharKeys[i];
                char keyVal[keyLen];
                memcpy(keyVal, leafNode.varcharKeys[i] + sizeof(keyLen), keyLen);
                std::cout << "Keys already present while deleting: " << keyVal[0] << " " << keyVal[keyLen-1] << std::endl;
//                std::cout << "Searching key : " << *((char*)searchKey + 4) << " " << *((char*)searchKey + 4 + *(int*)searchKey) << std::endl;
//                std::cout << "While finding " << keyLen << std::endl;
                if (memcmp(leafNode.varcharKeys[i], searchKey, keyLen + sizeof (keyLen)) == 0)
                    if (leafNode.rids[i].pageNum == rid.pageNum)
                        if (leafNode.rids[i].slotNum == rid.slotNum)
                            return i;
            }
        }
        return -1;
    }

    int search(PageNum rootPtr, const void* key, IXFileHandle &iXFileHandle, const Attribute &attribute) {
        std::cout << "Searching Page " << rootPtr << std::endl;
        void *desPage[PAGE_SIZE];
        memset(desPage, 0, PAGE_SIZE);
        iXFileHandle.iXReadPage(rootPtr, desPage);
        int returnedIndex = -1;

        if (*((char *) desPage + IS_LEAF_OFFSET)) {
            return rootPtr;
        }
        else {
            InternalNode internalNode;
            internalNode.Deserialize(desPage, attribute);
            // find key
            // recursive call with returned key
//            PageNum returnedPage = findChildIndex(internalNode, key, attribute);
            PageNum returnedPage = -1;
            returnedPage = findChildPointer(internalNode, key, attribute);
            return search(returnedPage, key, iXFileHandle, attribute);
        }
        //return returnedIndex;
//        }
    }

    template<typename KeyType>
    void copyKeys(char*& offsetPointer, const std::vector<KeyType>& keys, const std::vector<RID>& rids,
                  const std::vector<PageNum>& children) {
        assert(keys.size() + 1 == children.size() || children.empty()); // Ensure children size is correct for internal nodes

        for (size_t i = 0; i < keys.size(); ++i) {
            // Handle keys
            if (keys[i] == -1)
                continue;

            memcpy(offsetPointer, &keys[i], sizeof(KeyType));
            offsetPointer += sizeof(KeyType);

            // Copy RIDs for leaf nodes
            if (!rids.empty()) {
                memcpy(offsetPointer, &rids[i].pageNum, sizeof(rids[i].pageNum));
                offsetPointer += sizeof(rids[i].pageNum);
                memcpy(offsetPointer, &rids[i].slotNum, sizeof (rids[i].slotNum));
                offsetPointer += sizeof(rids[i].slotNum);
            }

            // Copy children page numbers for internal nodes
            if (!children.empty()) {
                memcpy(offsetPointer, &children[i], sizeof(PageNum));
                offsetPointer += sizeof(PageNum);
            }
        }

        // For internal nodes, also copy the last child
        if (!children.empty()) {
            memcpy(offsetPointer, &children.back(), sizeof(PageNum));
            offsetPointer += sizeof(PageNum);
        }
    }

    template<>
    void copyKeys<char*>(char*& offsetPointer, const std::vector<char*>& keys, const std::vector<RID>& rids,
                         const std::vector<PageNum>& children) {
        for (size_t i = 0; i < keys.size(); ++i) {
            if (keys[i] == nullptr)
                continue;

            int keyLen = *(int*)keys[i];
            memcpy(offsetPointer, &keyLen, sizeof(int));
            offsetPointer += sizeof(int);
            memcpy(offsetPointer, keys[i] + sizeof(int), keyLen);
            offsetPointer += keyLen;

            // Copy RIDs for leaf nodes
            if (!rids.empty()) {
                memcpy(offsetPointer, &rids[i].pageNum, sizeof(rids[i].pageNum));
                offsetPointer += sizeof(rids[i].pageNum);
                memcpy(offsetPointer, &rids[i].slotNum, sizeof (rids[i].slotNum));
                offsetPointer += sizeof(rids[i].slotNum);
            }

            // Copy children page numbers for internal nodes
            if (!children.empty() && i < children.size()) {
                memcpy(offsetPointer, &children[i], sizeof(PageNum));
                offsetPointer += sizeof(PageNum);
            }
        }

        // For internal nodes, also copy the last child
        if (!children.empty()) {
            memcpy(offsetPointer, &children.back(), sizeof(PageNum));
            offsetPointer += sizeof(PageNum);
        }
    }

    template<typename KeyType>
    void populateKeys(char*& offsetPointer, std::vector<KeyType>& keys, std::vector<RID>& rids,
                      std::vector<PageNum>& children, int numKeys, bool isLeaf) {

        keys.clear();
        rids.clear();

        for (int i = 0; i < numKeys; ++i) {
            KeyType key;
            memcpy(&key, offsetPointer, sizeof(KeyType));
            offsetPointer += sizeof(KeyType);
            keys.push_back(key);

            // Populate RIDs for leaf nodes
            if (isLeaf) {
                RID rid;
                memcpy(&rid.pageNum, offsetPointer, sizeof(rid.pageNum));
                offsetPointer += sizeof(rid.pageNum);
                memcpy(&rid.slotNum, offsetPointer, sizeof(rid.slotNum));
                offsetPointer += sizeof(rid.slotNum);
                rids.push_back(rid);
            }

            // For internal nodes, there's a child associated with every key
            if (!isLeaf) { // Don't read past the keys for children
                PageNum childPage;
                memcpy(&childPage, offsetPointer, sizeof(PageNum));
                offsetPointer += sizeof(PageNum);
                children.push_back(childPage);
            }
        }

        // For internal nodes, get the last child page number
        if (!children.empty()) {
            PageNum childPage;
            memcpy(&childPage, offsetPointer, sizeof(PageNum));
            offsetPointer += sizeof(PageNum);
            children.push_back(childPage);
        }
    }

    template<>
    void populateKeys<char*>(char*& offsetPointer, std::vector<char*>& keys, std::vector<RID>& rids,
                             std::vector<PageNum>& children, int numKeys, bool isLeaf) {
        keys.clear();
        rids.clear();

        for (int i = 0; i < numKeys; ++i) {
            int keyLen;
            memcpy(&keyLen, offsetPointer, sizeof(int));
            keys.push_back(offsetPointer);

            offsetPointer += sizeof(int) + keyLen;


            // Populate RIDs for leaf nodes
            if(isLeaf){
                RID rid;
                memcpy(&rid.pageNum, offsetPointer, sizeof(rid.pageNum));
                offsetPointer += sizeof(rid.pageNum);
                memcpy(&rid.slotNum, offsetPointer, sizeof(rid.slotNum));
                offsetPointer += sizeof(rid.slotNum);
                rids.push_back(rid);
            }

            // For internal nodes, there's a child associated with every key
            if (!isLeaf) {
                PageNum childPage;
                memcpy(&childPage, offsetPointer, sizeof(PageNum));
                offsetPointer += sizeof(PageNum);
                children.push_back(childPage);
            }
        }

        // For internal nodes, get the last child page number
        if (!children.empty()) {
            PageNum childPage;
            memcpy(&childPage, offsetPointer, sizeof(PageNum));
            offsetPointer += sizeof(PageNum);
            children.push_back(childPage);
        }
    }


    void LeafNode::Serialize(void* data, const Attribute &attribute) {
        *(int*)((char*)data + FREE_SPACE_OFFSET) = freeSpace;
        *(int*)((char*)data + NUM_KEYS_OFFSET) = numKeys;
        *(char*)((char*)data + IS_LEAF_OFFSET) = isLeaf;
        char* offsetPointer = (char*)data ; // Advance past header

        switch (attribute.type) {
            case TypeInt:
                copyKeys<int>(offsetPointer, intKeys, rids, std::vector<PageNum>());
                break;
            case TypeReal:
                copyKeys<float>(offsetPointer, floatKeys, rids, std::vector<PageNum>());
                break;
            case TypeVarChar:
                copyKeys<char*>(offsetPointer, varcharKeys, rids, std::vector<PageNum>());
                break;
        }
    }

    void LeafNode::Deserialize(const void* data, const Attribute &attribute) {
        freeSpace = *(int*)((char*)data + FREE_SPACE_OFFSET);
        numKeys = *(int*)((char*)data + NUM_KEYS_OFFSET);
        isLeaf = *(char*)((char*)data + IS_LEAF_OFFSET);
        char* offsetPointer = (char*)data;
        std::vector<PageNum> emptyChildren;

        switch (attribute.type) {
            case TypeInt:
                populateKeys<int>(offsetPointer, intKeys, rids, emptyChildren, numKeys, true);
                break;
            case TypeReal:
                populateKeys<float>(offsetPointer, floatKeys, rids,emptyChildren, numKeys, true);
                break;
            case TypeVarChar:
                populateKeys<char*>(offsetPointer, varcharKeys, rids, emptyChildren, numKeys, true);
                break;
        }
    }

    void InternalNode::Serialize(void* data, const Attribute &attribute) {
        *(int*)((char*)data + FREE_SPACE_OFFSET) = freeSpace;
        *(int*)((char*)data + NUM_KEYS_OFFSET) = numKeys;
        *(char*)((char*)data + IS_LEAF_OFFSET) = isLeaf;
        char* offsetPointer = (char*)data; // Advance past header

        // Note: Internal nodes do not have RIDs
        switch (attribute.type) {
            case TypeInt:
                copyKeys<int>(offsetPointer, intKeys, std::vector<RID>(), children);
                break;
            case TypeReal:
                copyKeys<float>(offsetPointer, floatKeys, std::vector<RID>(), children);
                break;
            case TypeVarChar:
                copyKeys<char*>(offsetPointer, varcharKeys, std::vector<RID>(), children);
                break;
        }
    }

    // InternalNode::Deserialize
    void InternalNode::Deserialize(const void* data, const Attribute &attribute) {
        freeSpace = *(int*)((char*)data + FREE_SPACE_OFFSET);
        numKeys = *(int*)((char*)data + NUM_KEYS_OFFSET);
//        isLeaf = *(char*)((char*)data + IS_LEAF_OFFSET);
        char* offsetPointer = (char*)data;
        std::vector<RID> emptyRids;


        // Note: Internal nodes do not have RIDs
        switch (attribute.type) {
            case TypeInt:
                populateKeys<int>(offsetPointer, intKeys, emptyRids, children, numKeys, false);
                break;
            case TypeReal:
                populateKeys<float>(offsetPointer, floatKeys, emptyRids, children, numKeys, false);
                break;
            case TypeVarChar:
                populateKeys<char*>(offsetPointer, varcharKeys, emptyRids, children, numKeys, false);
                break;
        }
    }

    RC IndexManager::createFile(const std::string &fileName) {
        if(PagedFileManager::instance().createFile(fileName)){
            return -1;
        }
        return 0;
    }


    RC IndexManager::destroyFile(const std::string &fileName) {
        if(PagedFileManager::instance().destroyFile(fileName)){
            return -1;
        }
        return 0;
    }


    RC IndexManager::openFile(const std::string &fileName, IXFileHandle &iXFileHandle) {
        if(iXFileHandle.isOpen){
            std::cout << "File already opened" <<std::endl;
            return -1;
        }
        if(PagedFileManager::instance().openFile(fileName, iXFileHandle.fileHandle)){
            return -1;
        }
        iXFileHandle.isOpen = true;
        return 0;
    }


    RC IndexManager::closeFile(IXFileHandle &iXFileHandle) {
        if(PagedFileManager::instance().closeFile(iXFileHandle.fileHandle)){
            return -1;
        }
        iXFileHandle.isOpen = false;
        return 0;
    }

    RC
    IndexManager:: insertEntry(IXFileHandle &iXFileHandle, const Attribute &attribute, const void *key, const RID &rid) {

        if(iXFileHandle.dummyHead == -1){
            // create new b plus tree
            LeafNode leafNode;
            createBPlusTree(iXFileHandle, leafNode, attribute);
        }
        PageNum newChildPageNum;
        void *smallestKey;
        insert(iXFileHandle.dummyHead, newChildPageNum, smallestKey,
               key, rid, iXFileHandle, attribute);

        return 0;
    }
    void markKeyAsInvalid (LeafNode &leafNode, int index, const Attribute &attribute) {
        int keyLen = 0;
        switch (attribute.type) {
            case TypeVarChar:
                keyLen = *(int*)leafNode.varcharKeys[index];
                leafNode.freeSpace -= (keyLen + sizeof (keyLen));
                leafNode.varcharKeys[index] = nullptr;
                break;

            case TypeInt:
                leafNode.intKeys[index] = -1;
                leafNode.freeSpace -= sizeof (int);
                break;
            case TypeReal:
                leafNode.floatKeys[index] = -1;
                leafNode.freeSpace -= sizeof (float);
                break;
        }
        leafNode.numKeys--;
    }

    RC deleteNode (PageNum nodePtr, const Attribute &attribute, IXFileHandle &iXFileHandle,
                     const void* key, const RID &rid) {

        readDummyHeadFromFile(iXFileHandle);
        char desPage[PAGE_SIZE];
        char sePage[PAGE_SIZE];
        memset(desPage, 0, PAGE_SIZE);
        memset(sePage, 0, PAGE_SIZE);
        iXFileHandle.iXReadPage(nodePtr, desPage);
        char isLeaf = *(desPage + IS_LEAF_OFFSET);

        if (isLeaf) {
            LeafNode leafNode;
            leafNode.Deserialize(desPage, attribute);
            int returnedIndex = -1;
            returnedIndex = findKeyDelete(leafNode, key, attribute, rid);
            if (returnedIndex == -1)
                return -1;

            markKeyAsInvalid(leafNode, returnedIndex, attribute);
            leafNode.Serialize(sePage, attribute);
            iXFileHandle.iXWritePage(nodePtr, sePage);
        }
        else {
            InternalNode internalNode;
            internalNode.Deserialize(desPage, attribute);
            int returnedPage = -1;
            returnedPage = findChildPointer(internalNode, key, attribute);
            deleteNode(returnedPage, attribute, iXFileHandle, key, rid);
        }
        return 0;
    }
    RC
    IndexManager::deleteEntry(IXFileHandle &iXFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        return deleteNode(iXFileHandle.dummyHead, attribute, iXFileHandle, key, rid);
//        int returned_index = search(iXFileHandle.dummyHead, key, iXFileHandle, attribute);
//        std::cout << " Index " << returned_index << std::endl;
    }

    void storeNullValues (void *&storedKey, const Attribute &attribute) {
        storedKey = malloc(sizeof (int));
        memset(storedKey, 0, sizeof (int));
        int invalidInt = -1;
        float invalidFloat = -1;
        int check;

        switch (attribute.type) {
            case TypeInt:
                memcpy(storedKey, &invalidInt, sizeof (int));
                check = *(int*)storedKey;
                break;
            case TypeReal:
                memcpy(storedKey, &invalidFloat, sizeof (float));
                break;
            case TypeVarChar:
                break;
        }
    }
    void storeConditionToScanIterator(const void *key, void *&storedKey, const Attribute &attribute, bool isLow) {
        if (key == nullptr) {
            if (isLow) {
                storeNullValues(storedKey, attribute);
                return;
            } else {
                storedKey = nullptr;
                return;
            }
        }

        int size = 0;

        switch (attribute.type) {
            case TypeVarChar:
                size += *(int*)key + sizeof (int);
                break;
            case TypeReal:
            case TypeInt:
                size += sizeof (int);
                break;
        }

        storedKey = malloc(size);
        memset(storedKey, 0, size);
        memcpy(storedKey, key, size);
    }

    void copyToReturnKey (void *key, RID &rid, Attribute attribute, LeafNode &leafNode, int currentIndex) {
        int size = 0;
        switch (attribute.type) {
            case TypeInt:
                memcpy(key, &leafNode.intKeys[currentIndex], sizeof (int));
                break;
            case TypeReal:
                memcpy(key, &leafNode.floatKeys[currentIndex], sizeof (float));
                break;
            case TypeVarChar:
                size = *(int *) key;
                size += sizeof(int);
                memcpy(key, leafNode.varcharKeys[currentIndex], size);
                break;
        }
        rid.pageNum = leafNode.rids[currentIndex].pageNum;
        rid.slotNum = leafNode.rids[currentIndex].slotNum;
    }

    bool areYouHigh (void *key, void *highKey, bool highKeyInclusive, Attribute &attribute) {
        if (highKey == nullptr)
            return false;

        int keyLen;
        int highKeyLen;
        std::string keyStr;
        std::string highKeyStr;

        switch (attribute.type) {
            case TypeVarChar:
                keyLen = *(int *) key;
                highKeyLen = *(int *) highKey;
                keyStr = std::string((char *) key + sizeof(int), keyLen);
                highKeyStr = std::string((char *) highKey + sizeof(int), highKeyLen);

                if (highKeyInclusive) {
                    if (keyStr >= highKeyStr)
                        return true;
                } else {
                    if (keyStr > highKeyStr)
                        return true;
                }
                break;

            case TypeReal:
                if (highKeyInclusive) {
                    if (*(float *) key >= *(float *) highKey)
                        return true;
                } else {
                    if (*(float *) key > *(float *) highKey)
                        return true;
                }
                break;
            case TypeInt:
                if (highKeyInclusive) {
                    if (*(int *) key >= *(int *) highKey)
                        return true;
                } else {
                    if (*(int *) key > *(int *) highKey)
                        return true;
                }
                break;
        }
    }
    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {

        if (currentIndex == -1)
            return IX_EOF;

        copyToReturnKey(key, rid, attribute, leafNode, currentIndex);
        if (areYouHigh (key, highKey, highKeyInclusive, attribute))
            return IX_EOF;

        currentIndex++;
        if (currentIndex == leafNode.numKeys) {
            if (leafNode.next == -1)
                currentIndex = -1;
            else {
                char desPage [PAGE_SIZE];
                ixFileHandle.iXReadPage(leafNode.next, desPage);
                leafNode.Deserialize(desPage, attribute);

                currentIndex = 0;
            }
        }
        return 0;
    }

    RC IndexManager::scan(IXFileHandle &iXFileHandle,
                          const Attribute &attribute,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator) {
        if (!iXFileHandle.isOpen)
            return -1;

        ix_ScanIterator.lowKeyInclusive = lowKeyInclusive;
        ix_ScanIterator.highKeyInclusive = highKeyInclusive;
        ix_ScanIterator.attribute = attribute;
        ix_ScanIterator.ixFileHandle = iXFileHandle;

        storeConditionToScanIterator(lowKey,ix_ScanIterator.lowKey, attribute, true);
        storeConditionToScanIterator(highKey,ix_ScanIterator.highKey, attribute, false);

        //search for the node containing the lowkey
        int returnedPage = search(iXFileHandle.dummyHead, ix_ScanIterator.lowKey, iXFileHandle, attribute);
        if (returnedPage == -1)
            return -1;

        char desPage [PAGE_SIZE];
        iXFileHandle.iXReadPage(returnedPage, desPage);
        ix_ScanIterator.leafNode.Deserialize(desPage, attribute);
        if (lowKeyInclusive)
            ix_ScanIterator.currentIndex = findKey(&ix_ScanIterator.leafNode, ix_ScanIterator.lowKey, attribute, GE_OP);
        else
            ix_ScanIterator.currentIndex = findKey(&ix_ScanIterator.leafNode, ix_ScanIterator.lowKey, attribute, GT_OP);

        return 0;
    }

    void printNode(Node* node, const Attribute  &attribute, std::ostream& out, IXFileHandle &iXFileHandle);

    void printInternalNode(const InternalNode* internalNode, const Attribute &attribute, std::ostream& out, IXFileHandle &iXFileHandle) {
        out << "{\"keys\":[";
        switch(attribute.type){

            case TypeInt:
                for (size_t i = 0; i < internalNode->intKeys.size(); ++i) {
                    out << "\"" << internalNode->intKeys[i] << "\"";
                    if (i < internalNode->intKeys.size() - 1) out << ", ";
                }
                out << "], \"children\": [";
                for (size_t i = 0; i < internalNode->children.size(); ++i) {
                    Node* node;
                    char nodePage[PAGE_SIZE];
                    iXFileHandle.iXReadPage(internalNode->children[i], nodePage);
                    *((char *) nodePage + IS_LEAF_OFFSET) ? node = new LeafNode() : node = new InternalNode();
                    node->Deserialize(nodePage, attribute);
                    printNode(node, attribute, out, iXFileHandle);
                    if (i < internalNode->children.size() - 1) out << ", ";
                }
                out << "]}";
                break;

            case TypeReal:
                for (size_t i = 0; i < internalNode->floatKeys.size(); ++i) {
                    out << "\"" << internalNode->floatKeys[i] << "\"";
                    if (i < internalNode->floatKeys.size() - 1) out << ", ";
                }
                out << "], \"children\": [";
                for (size_t i = 0; i < internalNode->children.size(); ++i) {
                    Node* node;
                    char nodePage[PAGE_SIZE];
                    iXFileHandle.iXReadPage(internalNode->children[i], nodePage);
                    *((char *) nodePage + IS_LEAF_OFFSET) ? node = new LeafNode() : node = new InternalNode();
                    node->Deserialize(nodePage, attribute);
                    printNode(node, attribute, out, iXFileHandle);
                    if (i < internalNode->children.size() - 1) out << ", ";
                }
                out << "]}";
                break;

            case TypeVarChar:
                for (size_t i = 0; i < internalNode->varcharKeys.size(); ++i) {
                    int keyLen = *(int*)internalNode->varcharKeys[i];
                    std::string keyStr(internalNode->varcharKeys[i]+sizeof(int), keyLen);
                    out << "\"" << keyStr << "\"";
//                out << "]}";
                    if (i < internalNode->varcharKeys.size() - 1) out << ", ";
                }
                out << "], \"children\": [";
                for (size_t i = 0; i < internalNode->children.size(); ++i) {
                    Node* node;
                    char nodePage[PAGE_SIZE];
                    iXFileHandle.iXReadPage(internalNode->children[i], nodePage);
                    *((char *) nodePage + IS_LEAF_OFFSET) ? node = new LeafNode() : node = new InternalNode();
                    node->Deserialize(nodePage, attribute);
                    printNode(node, attribute, out, iXFileHandle);
                    if (i < internalNode->children.size() - 1) out << ", ";
                }
                out << "]}";
                break;

        }
    }

    void printLeafNode(const LeafNode* leafNode, const Attribute &attribute, std::ostream& out) {
        out << "{\"keys\": [";
        switch(attribute.type){
            case TypeInt:
                for (size_t i = 0; i < leafNode->intKeys.size();) {
                    out << "\"" << leafNode->intKeys[i] << ":[";
                    size_t j = i;
                    while (j < leafNode->rids.size() && leafNode->intKeys[j] == leafNode->intKeys[i]) {
                        out << "(" << leafNode->rids[j].pageNum << "," << leafNode->rids[j].slotNum << ")";
                        if (j < leafNode->rids.size() - 1 && leafNode->intKeys[j] == leafNode->intKeys[j + 1]) {
                            out << ",";
                        }
                        ++j;
                    }
                    out << "]\"";

                    i = j;
                    if (i < leafNode->intKeys.size()) out << ", ";
                }
                out << "]}";
                break;

            case TypeReal:
                for (size_t i = 0; i < leafNode->floatKeys.size();) {
                    out << "\"" << leafNode->floatKeys[i] << ":[";
                    size_t j = i;
                    while (j < leafNode->rids.size() && leafNode->floatKeys[j] == leafNode->floatKeys[i]) {
                        out << "(" << leafNode->rids[j].pageNum << "," << leafNode->rids[j].slotNum << ")";
                        if (j < leafNode->rids.size() - 1 && leafNode->floatKeys[j] == leafNode->floatKeys[j + 1]) {
                            out << ",";
                        }
                        ++j;
                    }
                    out << "]\"";

                    i = j;
                    if (i < leafNode->floatKeys.size()) out << ", ";
                }
                out << "]}";
                break;

            case TypeVarChar:
                for (size_t i = 0; i < leafNode->varcharKeys.size();) {
                    int keyLen = *(int*)leafNode->varcharKeys[i];
                    std::string keyStr(leafNode->varcharKeys[i]+sizeof(int), keyLen);
                    out << "\"" << keyStr << ":[";
                    size_t j = i;
                    while (j < leafNode->rids.size()) {
                        int nextKeyLen = *(int*)leafNode->varcharKeys[j];
                        std::string nextKeyStr(leafNode->varcharKeys[j]+sizeof(int), nextKeyLen);
                        if(keyStr == nextKeyStr){
                            out << "(" << leafNode->rids[j].pageNum << "," << leafNode->rids[j].slotNum << ")";
                            if (j < leafNode->rids.size() - 1) {
                                int nextToNextKeyLen = *(int*)leafNode->varcharKeys[j+1];
                                std::string nextToNextKeyStr(leafNode->varcharKeys[j+1]+sizeof(int), nextToNextKeyLen);
                                if(nextKeyStr == nextToNextKeyStr){
                                    out << ",";
                                }
                            }
                        }
                        else{
                            break;
                        }
                        ++j;
                    }
                    out << "]\"";

                    i = j;
                    if (i < leafNode->varcharKeys.size()) out << ", ";
                }
                out << "]}";
                break;
        }

    }

    void printNode(Node* node, const Attribute  &attribute, std::ostream& out, IXFileHandle &iXFileHandle) {
        if(node->isLeaf){
            printLeafNode(dynamic_cast<LeafNode *>(node), attribute,  out);
        }
        else{
            printInternalNode(dynamic_cast<InternalNode *>(node), attribute, out, iXFileHandle);
        }
    }


    RC IndexManager::printBTree(IXFileHandle &iXFileHandle, const Attribute &attribute, std::ostream &out) const {
        Node* rootNode;
        char rootPage[PAGE_SIZE];
        iXFileHandle.iXReadPage(iXFileHandle.dummyHead, rootPage);

        *((char *) rootPage + IS_LEAF_OFFSET) ? rootNode = new LeafNode() : rootNode = new InternalNode();
        rootNode->Deserialize(rootPage, attribute);

        printNode(rootNode, attribute, out, iXFileHandle);
        return 0;
    }
//

    IX_ScanIterator::IX_ScanIterator() {
    }


    IX_ScanIterator::~IX_ScanIterator() {
    }

    RC IX_ScanIterator::close() {
        if (lowKey)
            free(lowKey);
        if (highKey)
            free(highKey);

        return 0;
    }


    IXFileHandle::IXFileHandle() {
        ixReadPageCounter = 0;
        ixWritePageCounter = 0;
        ixAppendPageCounter = 0;
        dummyHead = -1;
    }


    IXFileHandle::~IXFileHandle() {
    }

    RC
    IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = ixReadPageCounter;
        writePageCount = ixWritePageCounter;
        appendPageCount = ixAppendPageCounter;
        return 0;
    }
} // namespace PeterDB
