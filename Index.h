#pragma once

#ifndef MYINDEX
#define MYINDEX
#include "Table.h"
#include <vector>
/*
索引是一个特殊的顺序文件组织的表，通过输入某一属性值，
索引能够返回所有具有该属性值的元组的地址。
索引的建立是通过输入<属性值，元组地址>，
内部在属性值对应的块上加入一项元组地址。
其数据项的格式如下
（1）顺序索引
一个磁盘块内可以存储多个不同属性值对应的元组地址，
因此需要附加属性值：
<unsigned char[] attr_val, unsigned blk_num, unsigned offset>
sorted by attr_val
（2）哈希索引
一个磁盘块内仅仅存放具有相同hash值的元组地址，
由于具有相同hash值的元组，其属性值不一定相同，因此同样需要附加属性值，
值得一提的是，由于一个磁盘块不一定能够存下所有元组的属性值和地址（桶溢出），
必要时将通过磁盘块的next属性，链接到下一磁盘块
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
