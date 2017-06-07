#pragma once
#ifndef MYDB
#define MYDB
#include "Table.h"
#include "Index.h"
#include "BufferManager.h"
#include <vector>
#include <string>
#include <unordered_map>
#include <stdexcept>

/*
���ݿ�
Table��Index�ļ����Լ�����Ĳ���
*/

class DB
{
public:
	DB(BufferManager& _bm):bm(_bm){}
	~DB();
	//����������д���̡����ٲ���
	Table& createSeqTable(std::string tableName, unsigned _searchKey, std::vector<unsigned> _cols, bool _cmp);
	Index& createHashIndexOn(std::string indexName, std::string tableName, unsigned attr, unsigned _bucketSize);
	/*
	void saveTable(std::string tableName);
	void saveIndex(std::string indexName);
	void saveDB();
	void dropTable(std::string tableName);
	void dropIndex(std::string indexName);
	void dropDB();
	*/
	//���Ԫ��
	int insertInto(std::string tableName, std::map<unsigned, unsigned char*> cond, bool distinct);
	//ɾ��Ԫ��
	int removeFrom(std::string tableName, std::map<unsigned, unsigned char*> cond);
	//��ϵѡ��
	std::vector<unsigned char*> selectFrom(std::string tableName, std::map<unsigned, unsigned char*> cond); 
	//��ϵ����
	/*
	std::vector<unsigned char*> selectJoin
	(std::pair<std::string, std::string>tableNames, std::vector<std::pair<unsigned, unsigned>> cond);
	//ͶӰ
	std::vector<unsigned char*> projectOf(std::vector<unsigned char*>& rawResult, std::vector<std::pair<unsigned, unsigned>> cond);
	//������
	std::vector<unsigned char*> unionOf(std::pair<std::vector<unsigned char*>&, std::vector<unsigned char*>&> tables);
	//������
	std::vector<unsigned char*> intersectOf(std::pair<std::vector<unsigned char*>&, std::vector<unsigned char*>&> tables);
	//�������
	std::vector<unsigned char*> diffOf(std::pair<std::vector<unsigned char*>&, std::vector<unsigned char*>&> tables);
	*/
private:
	std::unordered_map<std::string, SeqTable*> tables;
	std::unordered_map<std::string, HashIndex*> indexes;
	//map from table_name to (attr, index_name)
	std::unordered_map<std::string, std::vector<std::pair<unsigned, std::string>>> tabIdx;
	BufferManager &bm;

	//helper functions
	int doRemove(std::string tableName, unsigned char* &blk, std::vector<std::pair<unsigned char*, std::pair<unsigned, unsigned>>> args, unsigned addr);
};

#endif // !MYDB
