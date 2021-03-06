#include "Index.h"

Index::Index(unsigned _metaAddr, BufferManager & _bm) :Table(_metaAddr, _bm)
{
	attrSize = cols[1];
}

Index::Index(BufferManager & _bm, unsigned _attrSize)
	: Table(_bm, _attrSize + 4, 1, std::vector<unsigned>({ 0, _attrSize, _attrSize + 4 })), attrSize(_attrSize)
{
}

HashIndex::HashIndex(unsigned _metaAddr, BufferManager & _bm):Index(_metaAddr, _bm)
{
	unsigned char* blk = _bm.read(_metaAddr);
	bucketSize = *(unsigned*)(blk + 24);
}

HashIndex::HashIndex(BufferManager & _bm, unsigned _attrSize, unsigned _bucketSize)
	:Index(_bm, _attrSize), bucketSize(_bucketSize)
{
	//初始化桶
	index.resize(bucketSize);
	for (unsigned i = 1; i < bucketSize; ++i)
	{
		unsigned addr = getFreeBlockOnDisk();
		unsigned char* blk = bm.get(addr);
		*(unsigned*)blk = 8;
		*(unsigned*)(blk + 4) = -1;
		bm.write(addr);
		++numBlk;
		//更新索引
		index[i] = addr;
	}
}

std::vector<unsigned> HashIndex::get(unsigned char * attr)
{
	std::vector<unsigned> res;

	std::vector<std::pair<unsigned char*, std::pair<unsigned, unsigned>>> cond{ {attr, {0, attrSize}} };
	unsigned char *blk;
	for (unsigned addr = index[hash(attr)]; addr != -1; addr = getNextAddr(blk))
	{
		auto tmp = doSelect(blk, cond, addr);
		for (auto tuple : tmp)
		{
			res.emplace_back(*(unsigned*)(tuple + attrSize));
		}
		/*
		原始版本
		blk = bm.read(addr);
		for (unsigned char* p = blk + 8; p != *(unsigned*)blk + blk; p += tupleSize)
		{
			if (memcmp(p, attr, attrSize) == 0)
			{
				res.push_back({ *(unsigned*)(p + attrSize), *(unsigned*)(p + attrSize + 4) });
			}
		}
		*/
	}
	return res;
}

int HashIndex::insert(unsigned char * attr, unsigned addr)
{
	unsigned bucket = index[hash(attr)];
	unsigned char rawAddr[4];
	*(unsigned*)rawAddr = addr;
	format(cache, { {1, attr}, {2, rawAddr} });
	return rawAdd(cache, bucket, 8).first;
}

int HashIndex::remove(unsigned char * attr, unsigned addr)
{
	unsigned bucket = index[hash(attr)];
	unsigned char rawAddr[4];
	*(unsigned*)rawAddr = addr;
	format(cache, { { 1, attr },{ 2, rawAddr }});
	int res = 0;
	for (unsigned char* blk; !(res = doRemove(blk, { {cache, {0, tupleSize}} }, bucket))
		&& (bucket = getNextAddr(blk)) != -1;);
	return res;
}

void HashIndex::save()
{
	Table::save();
	unsigned char* blk = bm.read(metaAddr);
	*(unsigned*)(blk + 24) = bucketSize;
	bm.write(metaAddr);
}

unsigned HashIndex::hash(unsigned char * attr)
{
	unsigned res = 0;
	unsigned *p = (unsigned*)attr;
	for (unsigned i = 0; i < attrSize / 4; ++i)
		res += *p++;
	return res % bucketSize;
}
