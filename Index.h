#pragma once

#ifndef MYINDEX
#define MYINDEX
#include "Table.h"
#include <vector>
/*
������һ�������˳���ļ���֯�ı�ͨ������ĳһ����ֵ��
�����ܹ��������о��и�����ֵ��Ԫ��ĵ�ַ��
�����Ľ�����ͨ������<����ֵ��Ԫ���ַ>��
�ڲ�������ֵ��Ӧ�Ŀ��ϼ���һ��Ԫ���ַ��
��������ĸ�ʽ����
��1��˳������
һ�����̿��ڿ��Դ洢�����ͬ����ֵ��Ӧ��Ԫ���ַ��
�����Ҫ��������ֵ��
<unsigned char[] attr_val, unsigned blk_num, unsigned offset>
sorted by attr_val
��2����ϣ����
һ�����̿��ڽ�����ž�����ͬhashֵ��Ԫ���ַ��
���ھ�����ͬhashֵ��Ԫ�飬������ֵ��һ����ͬ�����ͬ����Ҫ��������ֵ��
ֵ��һ����ǣ�����һ�����̿鲻һ���ܹ���������Ԫ�������ֵ�͵�ַ��Ͱ�������
��Ҫʱ��ͨ�����̿��next���ԣ����ӵ���һ���̿�
<unsigned char[] attr_val, unsigned blk_num, unsigned offset>
sorted by hash_val
*/

class Index : public Table
{
public:
	Index(unsigned _metaAddr, BufferManager & _bm);
	Index(BufferManager & _bm, unsigned _attrSize);
	virtual std::vector<unsigned> get(unsigned char* attr) = 0;
	virtual int insert(unsigned char* attr, unsigned addr) = 0;
	virtual int remove(unsigned char* attr, unsigned addr) = 0;

protected:
	unsigned attrSize;
};

class HashIndex : public Index
{
public:
	HashIndex(unsigned _metaAddr, BufferManager & _bm);
	HashIndex(BufferManager & _bm, unsigned _attrSize, unsigned _bucketSize);
	std::vector<unsigned> get(unsigned char* attr);
	int insert(unsigned char* attr, unsigned addr);
	int remove(unsigned char* attr, unsigned addr);
	void save();

private:
	unsigned bucketSize;
	unsigned hash(unsigned char* attr);
};




#endif // !MYINDEX
