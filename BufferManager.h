#pragma once
#ifndef BUFFERMANAGER_H
#define BUFFERMANAGER_H

#include "extmem.h"
#include <unordered_map>
#include <list>

/*
������������
ʵ�ֶ�buffer�ķ�װ
�ڲ����FIFO/LRU/MRU���滻�㷨
�ⲿ�ṩget/read+write�Ľӿ�
*/
class BufferManager
{
public:
	BufferManager(size_t bufSize, size_t blkSize);
	~BufferManager();
	virtual unsigned char* read(unsigned addr) = 0;
	virtual unsigned char* get(unsigned addr) = 0;
	bool write(unsigned addr);
	int getNumIO();
	unsigned getBlkSize();

protected:
	Buffer buffer;
	std::unordered_map<unsigned, std::pair<unsigned char*, bool>> LUT;
	bool full();
	virtual void swap() = 0;
};

class FIFOBufMgr : public BufferManager
{
public:
	FIFOBufMgr(size_t bufSize, size_t blkSize);
	unsigned char* read(unsigned addr);
	unsigned char* get(unsigned addr);

private:
	std::list<unsigned> q;
	std::unordered_map<unsigned, std::list<unsigned>::iterator> pos;
	void swap();
	unsigned char* doGetBlk(bool type, unsigned addr);
};

class LRUBufMgr : public BufferManager
{
public:
	LRUBufMgr(size_t bufSize, size_t blkSize);
	unsigned char* read(unsigned addr);
	unsigned char* get(unsigned addr);

private:
	int minFreq = 0;
	std::unordered_map<unsigned, int> freq;
	std::unordered_map<unsigned, std::list<unsigned>::iterator> pos;
	std::unordered_map<int, std::list<unsigned>> queue;
	void swap();
	unsigned char* doGetBlk(bool type, unsigned addr);
};

class MRUBufMgr : public BufferManager
{
public:
	MRUBufMgr(size_t bufSize, size_t blkSize);
	unsigned char* read(unsigned addr);
	unsigned char* get(unsigned addr);

private:
	int maxFreq = 0;
	std::unordered_map<unsigned, int> freq;
	std::unordered_map<unsigned, std::list<unsigned>::iterator> pos;
	std::unordered_map<int, std::list<unsigned>> queue;
	void swap();
	unsigned char* doGetBlk(bool type, unsigned addr);
};

#endif
