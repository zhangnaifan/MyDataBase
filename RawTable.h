#pragma once
#ifndef RAWTABLE_H
#define RAWTABLE_H
#include "BufferManager.h"
#include <vector>

/*
底层表
接口：提供字节层次的，直接操作磁盘块的表单CRUD（增删改查）接口
*/
class RawTable
{
public:
	//初始化方式一：从_metaAddr对应的磁盘中读取数据库的配置信息，详情见block.h头文件
	RawTable(unsigned addr, BufferManager &_bm);
	/*
	初始化方式二：在内存中根据参数直接初始化
	参数：
		_bm：表所关联的BufferManager，通过其进行IO操作
		_tupleSize：元组所占的字节数
		_cmp：进行线性搜索时的比较方式，true：字符串比较，false：4字节无符号整数比较
	*/
	RawTable(BufferManager &_bm, unsigned _tupleSize);
	~RawTable();

	/*
	功能：
		添加一个元组
	参数：
		tuple：元组
		addr：添加到的磁盘块地址，默认-1，将添加到最后一个磁盘块
		offset：添加到的磁盘块的块内偏移，默认-1，将添加到最后空闲处
	返回：
		-1：无新磁盘块被使用
		否则：新磁盘块地址
	*/
	std::pair<unsigned, std::pair<unsigned, unsigned>> rawAdd(unsigned char* tuple, unsigned addr = -1, unsigned offset = -1);
	
	/*
	功能：
		删除给定条件的元组
	参数：
		args：删除条件，通过字节比较，删除相同位置处（可能有多个）具有相同字节的元组
				其中外面的pair的前者代表字节内容，里面的pair代表该字节内容位于元组的（偏移，长度）
		addr：删除元组所在的磁盘块，默认-1，将遍历所有磁盘块
	返回：
		被删除的元组个数
	*/
	int rawRemove(std::vector<std::pair<unsigned char*, std::pair<unsigned, unsigned>>> args, unsigned addr = -1);
	/*
	功能：
		更新给定条件的元组
	参数：
		args：更新条件，通过字节比较，更新相同位置处（可能有多个）具有相同字节的元组
				其中外面的pair的前者代表字节内容，里面的pair代表该字节内容位于元组的（偏移，长度）
		after：替换的元组
		addr：删除元组所在的磁盘块，默认-1，将遍历所有磁盘块
	返回：
		被更新的元组个数
	*/
	int rawUpdate(std::vector<std::pair<unsigned char*, std::pair<unsigned, unsigned>>> args, unsigned char* after, unsigned addr = -1);
	/*
	功能：
		返回符合给定条件的元组
	参数：
		args：筛选条件，通过字节比较，筛选相同位置处（可能有多个）具有相同字节的元组
			其中外面的pair的前者代表字节内容，里面的pair代表该字节内容位于元组的（偏移，长度）
		addr：筛选元组所在的磁盘块，默认-1，将遍历所有磁盘块
	返回：
		符合条件的元组指针数组
	*/
	std::vector<unsigned char*> rawSelect(std::vector<std::pair<unsigned char*, std::pair<unsigned, unsigned>>> args, unsigned addr = -1);
	
	//将表写入磁盘块，并将其地址返回
	unsigned save();
	void dropAll();

protected:
	unsigned getNextAddr(unsigned char*);
	int doRemove(unsigned char* &blk, std::vector<std::pair<unsigned char*, std::pair<unsigned, unsigned>>> args, unsigned addr);
	int doUpdate(unsigned char * &blk, std::vector<std::pair<unsigned char*, std::pair<unsigned, unsigned>>> args, unsigned char * after, unsigned addr);
	std::vector<unsigned char*> doSelect(unsigned char * &blk, std::vector<std::pair<unsigned char*, std::pair<unsigned, unsigned>>> args, unsigned addr);

	//meta info
	BufferManager& bm; //所关联的BufferManager
	unsigned startAddr; //起始地址
	unsigned tupleSize; //元组大小
	unsigned numBlk; //数据库占用的磁盘块
	unsigned metaAddr; //磁盘块上存储的数据库信息：startAddr/tupleSize/numBlk/continuous

	//helper functions
	bool full(unsigned char*);
	unsigned getFreeBlockOnDisk();

private:
	//helper buffer
	unsigned char *myBuf = new unsigned char[bm.getBlkSize()];
};

#endif