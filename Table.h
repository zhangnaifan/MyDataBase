#pragma once

#ifndef MYDBTABLE
#define MYDBTABLE
#include "RawTable.h"
#include <vector>
#include <map>

class Table : public RawTable
{
public:
	Table(BufferManager &_bm, unsigned _metaAddr);
	Table(BufferManager &_bm, int _tupleSize, unsigned _searchKey, std::vector<unsigned> _cols);
	~Table();
	int insert(std::map<unsigned, unsigned char*>, bool distinct);
	int remove(std::map<unsigned, unsigned char*>);
	std::vector<unsigned char*> select(std::map<unsigned, unsigned char*>);

protected:
	//meta info
	unsigned searchKey;
	std::vector<unsigned> cols;

	//important data structure
	//map from logical linked list addr to actual disc number
	std::vector<unsigned> index;

	//helper functions
	void format(unsigned char* blk, std::map<unsigned, unsigned char*> args);

private:

};


/*
˳���
�ӿڣ��ڵײ��Ļ����Ͻ�һ����װ���ṩ�����Բ�ε�˳�򣨰������������У���CRUD
*/
class SeqTable : public RawTable
{
public:
	SeqTable(BufferManager &_bm, unsigned _metaAddr);
	SeqTable(BufferManager &_bm, int _tupleSize, unsigned _searchKey, std::vector<unsigned> _cols, bool _cmp);
	~SeqTable();
	int insert(std::map<unsigned, unsigned char*>, bool distinct);
	int remove(std::map<unsigned, unsigned char*>);
	std::vector<unsigned char*> select(std::map<unsigned, unsigned char*>);

	//for testing
	std::vector<unsigned> binarySearch(unsigned char* cond);

protected:
	//meta info
	unsigned searchKey;
	std::vector<unsigned> cols;

	//important data structure
	//map from logical linked list addr to actual disc number
	std::vector<unsigned> index;

	//helper functions
	void format(unsigned char* blk, std::map<unsigned, unsigned char*> args);
	/*
	���ܣ�
	�������̿��ַ��Χ�ڵ���������
	������
	cond�������ֽ�ָ��
	beg��������Ԫ���е���ʼλ��
	end��������Ԫ���е���ֹλ��
	from����ʼ������ַ��Ĭ��-1���ӵ�һ�����̿鿪ʼ����
	steps�������Ĵ��̿���
	���أ�
	��ֹ�ıȽϽ�������̿��ַ������ƫ��
	*/
	std::pair<int, std::pair<unsigned, unsigned>>
		linearSerach(unsigned char* cond, unsigned beg, unsigned len, unsigned from, unsigned steps);


private:
	//helper cache
	unsigned char *cache = new unsigned char[bm.getBlkSize()];
};

#endif 
