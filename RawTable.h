#pragma once
#ifndef RAWTABLE_H
#define RAWTABLE_H
#include "BufferManager.h"
#include <vector>

/*
�ײ��
�ӿڣ��ṩ�ֽڲ�εģ�ֱ�Ӳ������̿�ı�CRUD����ɾ�Ĳ飩�ӿ�
*/
class RawTable
{
public:
	//��ʼ����ʽһ����_metaAddr��Ӧ�Ĵ����ж�ȡ���ݿ��������Ϣ�������block.hͷ�ļ�
	RawTable(BufferManager &_bm, unsigned _metaAddr);
	/*
	��ʼ����ʽ�������ڴ��и��ݲ���ֱ�ӳ�ʼ��
	������
		_bm������������BufferManager��ͨ�������IO����
		_tupleSize��Ԫ����ռ���ֽ���
		_cmp��������������ʱ�ıȽϷ�ʽ��true���ַ����Ƚϣ�false��4�ֽ��޷��������Ƚ�
	*/
	RawTable(BufferManager &_bm, unsigned _tupleSize, bool _cmp);
	~RawTable();

	/*
	���ܣ�
		���һ��Ԫ��
	������
		tuple��Ԫ��
		addr����ӵ��Ĵ��̿��ַ��Ĭ��-1������ӵ����һ�����̿�
		offset����ӵ��Ĵ��̿�Ŀ���ƫ�ƣ�Ĭ��-1������ӵ������д�
	���أ�
		-1�����´��̿鱻ʹ��
		�����´��̿��ַ
	*/
	unsigned rawAdd(unsigned char* tuple, unsigned addr = -1, unsigned offset = -1);
	
	/*
	���ܣ�
		ɾ������������Ԫ��
	������
		args��ɾ��������ͨ���ֽڱȽϣ�ɾ����ͬλ�ô��������ж����������ͬ�ֽڵ�Ԫ��
				���������pair��ǰ�ߴ����ֽ����ݣ������pair������ֽ�����λ��Ԫ��ģ�ƫ�ƣ����ȣ�
		addr��ɾ��Ԫ�����ڵĴ��̿飬Ĭ��-1�����������д��̿�
	���أ�
		��ɾ����Ԫ�����
	*/
	int rawRemove(std::vector<std::pair<unsigned char*, std::pair<unsigned, unsigned>>> args, unsigned addr = -1);
	/*
	���ܣ�
		���¸���������Ԫ��
	������
		args������������ͨ���ֽڱȽϣ�������ͬλ�ô��������ж����������ͬ�ֽڵ�Ԫ��
				���������pair��ǰ�ߴ����ֽ����ݣ������pair������ֽ�����λ��Ԫ��ģ�ƫ�ƣ����ȣ�
		after���滻��Ԫ��
		addr��ɾ��Ԫ�����ڵĴ��̿飬Ĭ��-1�����������д��̿�
	���أ�
		�����µ�Ԫ�����
	*/
	int rawUpdate(std::vector<std::pair<unsigned char*, std::pair<unsigned, unsigned>>> args, unsigned char* after, unsigned addr = -1);
	/*
	���ܣ�
		���ط��ϸ���������Ԫ��
	������
		args��ɸѡ������ͨ���ֽڱȽϣ�ɸѡ��ͬλ�ô��������ж����������ͬ�ֽڵ�Ԫ��
			���������pair��ǰ�ߴ����ֽ����ݣ������pair������ֽ�����λ��Ԫ��ģ�ƫ�ƣ����ȣ�
		addr��ɸѡԪ�����ڵĴ��̿飬Ĭ��-1�����������д��̿�
	���أ�
		����������Ԫ��ָ������
	*/
	std::vector<unsigned char*> rawSelect(std::vector<std::pair<unsigned char*, std::pair<unsigned, unsigned>>> args, unsigned addr = -1);
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
	std::pair<int, std::pair<unsigned, unsigned>> linearSerach(unsigned char* cond, unsigned beg, unsigned end, unsigned from = -1, unsigned steps = 1);
	//����д����̿飬�������ַ����
	unsigned saveTable();

protected:
	unsigned getNextAddr(unsigned char*);
	int doRemove(unsigned char* &blk, std::vector<std::pair<unsigned char*, std::pair<unsigned, unsigned>>> args, unsigned addr);
	int doUpdate(unsigned char * &blk, std::vector<std::pair<unsigned char*, std::pair<unsigned, unsigned>>> args, unsigned char * after, unsigned addr);
	std::vector<unsigned char*> doSelect(unsigned char * &blk, std::vector<std::pair<unsigned char*, std::pair<unsigned, unsigned>>> args, unsigned addr);

	//meta info
	BufferManager& bm; //��������BufferManager
	unsigned startAddr; //��ʼ��ַ
	unsigned tupleSize; //Ԫ���С
	unsigned numBlk; //���ݿ�ռ�õĴ��̿�
	unsigned metaAddr; //���̿��ϴ洢�����ݿ���Ϣ��startAddr/tupleSize/numBlk/continuous
	bool cmp; //�����ַ����Ƚ�=true������unsigned�Ƚ�=false

	//helper functions
	bool full(unsigned char*);
	unsigned getFreeBlockOnDisk();

private:
	//helper buffer
	unsigned char *myBuf = new unsigned char[bm.getBlkSize()];
};

#endif