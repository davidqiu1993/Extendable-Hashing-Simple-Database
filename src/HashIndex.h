#ifndef _HASHINDEX_H_
#define	_HASHINDEX_H_

#include "General.h"

namespace exth
{
	class indexNode
	{
	private:
		int _nodeID;		//��ϣ�����нڵ��ţ���ʼ��Ϊ-1
		int _bucketOffset;	//Ͱ��ƫ������һҳΪһ����λ����ʼ��Ϊ-1

	public:
		indexNode()
		{
			_nodeID = -1;
			_bucketOffset = -1;
		}

		indexNode(int nodeID, int bucketNum)
		{
			setNodeID(nodeID);
			setBucketOffset(bucketNum);
		}

		indexNode(const indexNode& origin)
		{
			this->_nodeID = origin._nodeID;
			this->_bucketOffset = origin._bucketOffset;
		}

		int getNodeID()
		{
			return _nodeID;
		}
		void setNodeID(int nodeID)
		{
			_nodeID = nodeID;
		}

		int getBucketOffset()
		{
			return _bucketOffset;
		}
		void setBucketOffset(int bucketOffset)
		{
			_bucketOffset = bucketOffset;
		}
	};

	class HashIndex
	{
	private:
		MemoryBuffer* _hashIndex;
		void *_hashIndexBuffer;	//���ڼ�¼��ϣҳ���ڴ��еĶ�Ӧ��������ʼ��ַ

		int _maxNum;		//һ������ҳ����ܹ�����������ڵ���
		int *_currentNum;	//��ǰ����ҳ�д���������ڵ���,�����Դ洢��ÿ����ϣ����ҳ��
		int _capacity;		//ÿ��Ͱ����(�ֽ�Ϊ��λ)

	public:
		// @Parameters:	dbController �����DBControllerʵ�������ڶ����ݿ����ݽ��в���
		// @Return:		(void)
		// @Function:	ͨ�������DBControllerʵ�������ڴ����µĹ�ϣ����ҳ�����������뵽��������
		HashIndex(DBController &dbController)
		{
			setHashIndex(dbController);
		}

		// @Parameters:	dbController �����DBControllerʵ�������ڶ����ݿ����ݽ��в�����pageNumber Ҫ��ȡ��Ͱ�����ݿ��е�ҳƫ����
		// @Return:		(void)
		// @Function:	ͨ�������DBControllerʵ��������ҳƫ������ȡ��Ӧ�Ĺ�ϣ����ҳ���������У��Ὣԭ��HashIndex�����е���Ϣ�����Ҳ����Զ�����ԭ����Ϣ
		HashIndex(DBController &dbController, int pageNumber)
		{
			setHashIndex(dbController, pageNumber);
		}

		HashIndex(const HashIndex& origin)
		{
			this->_hashIndex = origin._hashIndex;
			this->_hashIndexBuffer = origin._hashIndexBuffer;

			this->_maxNum = origin._maxNum;
			this->_currentNum = origin._currentNum;
			this->_capacity = origin._capacity;
		}

		HashIndex& operator =(const HashIndex& origin)
		{
			this->_hashIndex = origin._hashIndex;
			this->_hashIndexBuffer = origin._hashIndexBuffer;

			this->_maxNum = origin._maxNum;
			this->_currentNum = origin._currentNum;
			this->_capacity = origin._capacity;

			return (*this);
		}

		// @Parameters:	dbController �����DBController�������ڶ�ȡҳ
		// @Return:		(void)
		// @Function:	�����µĹ�ϣ����ҳ�����������뵽��������
		void setHashIndex(DBController &dbController)
		{
			int pageNumber = dbController.AppendNewPage(DatabaseIndexFile);
			_hashIndex = &dbController.GetBufferMappingToPage(DatabaseIndexFile, pageNumber);
			_hashIndexBuffer = _hashIndex->getBuffer();
			_capacity = _hashIndex->getCapacity();
			_maxNum = (_capacity - sizeof(int)) / sizeof(indexNode);
			_currentNum = (int*)_hashIndexBuffer;
			(*_currentNum) = 0;
		}

		// @Parameters:	dbController �����DBController�������ڶ�ȡҳ��pageNumber Ҫ��ȡ��ҳƫ����
		// @Return:		(void)
		// @Function:	����ҳƫ������ȡ��Ӧ�Ĺ�ϣ����ҳ���������У��Ὣԭ��HashIndex�����е���Ϣ�����Ҳ����Զ�����ԭ����Ϣ
		void setHashIndex(DBController &dbController, int pageNumber)
		{
			_hashIndex = &dbController.GetBufferMappingToPage(DatabaseIndexFile, pageNumber);
			_hashIndexBuffer = _hashIndex->getBuffer();
			_capacity = _hashIndex->getCapacity();
			_currentNum = (int*)_hashIndexBuffer;
			_maxNum = (_capacity - sizeof(int)) / sizeof(indexNode);
		}

		// @Parameters:	toInsert ��ʾ������Ĺ�ϣ�����ڵ���Ϣ
		// @Return:		boolֵ��������ɹ�����true�����򷵻�false
		// @Function:	���ݴ���Ľڵ���Ϣ�����µ������ڵ�
		bool insertIndex(indexNode &toInsert)
		{
			if((*_currentNum) < _maxNum)
			{
				char *insertPtr = (char*)_hashIndexBuffer + sizeof(int) + (*_currentNum) * sizeof(indexNode);
				memcpy(insertPtr, &toInsert, sizeof(indexNode));
				(*_currentNum)++;
				Save();
				return true;
			}
			else
			{
				return false;
			}
		}

		// @Parameters:	�����ص������ڵ��ڸ�ҳ�еı��(��1��ʼ)
		// @Return:		������Ҫ��������ڵ�
		// @Function:	���������ŷ�����Ӧ�������ڵ�ͷָ��
		indexNode* getIndexNode(int number)
		{
			if (number<=0 || number>_maxNum)
			{
				throw out_of_range("exth::DBController::GetBufferMappingToPage(DatabaseFile, int): The request indexNode offset exceeds the exact maximum number of indexNode.");
			}
			char *reutrnPtr = (char*)_hashIndexBuffer + sizeof(int) + sizeof(indexNode)*(number-1);
			return (indexNode*)reutrnPtr;
		}

		// @Parameters:	(void)
		// @Return:		A bool indicating whether the buffer is saved to the corresponding disk page successfully
		// @Function:	Save the buffer to the corresponding disk page indicated by the page number.
		void Save()
		{
			_hashIndex->Save();
		}

		// @Parameters:	(void)
		// @Return:		(void)
		// @Function:	Pin one pin on this memory buffer.
		void Pin()
		{
			_hashIndex->Pin();
		}

		// @Parameters:	(void)
		// @Return:		(void)
		// @Function:	Unpin one pin off this memory buffer.
		void Unpin()
		{
			_hashIndex->Unpin();
		}


		int getCurrentNum()
		{
			return *_currentNum;
		}
		void setCuurentNum(const int &currentNum)
		{
			*_currentNum = currentNum;
		}

		int getMaxNum()
		{
			return _maxNum;
		}
		int getCapacity()
		{
			return _capacity;
		}
	};
};
#endif	//_HASHINDEX_H_


