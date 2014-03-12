#ifndef _HASHINDEX_H_
#define	_HASHINDEX_H_

#include "General.h"

namespace exth
{
	class indexNode
	{
	private:
		int _nodeID;		//哈希索引中节点编号，初始化为-1
		int _bucketOffset;	//桶的偏移量，一页为一个单位，初始化为-1

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
		void *_hashIndexBuffer;	//用于记录哈希页在内存中的对应缓冲区起始地址

		int _maxNum;		//一个索引页最多能够储存的索引节点数
		int *_currentNum;	//当前索引页中存入的索引节点数,该属性存储在每个哈希索引页中
		int _capacity;		//每个桶容量(字节为单位)

	public:
		// @Parameters:	dbController 传入的DBController实例，用于对数据库内容进行操作
		// @Return:		(void)
		// @Function:	通过传入的DBController实例，用于创建新的哈希索引页，并将其载入到缓冲区中
		HashIndex(DBController &dbController)
		{
			setHashIndex(dbController);
		}

		// @Parameters:	dbController 传入的DBController实例，用于对数据库内容进行操作，pageNumber 要读取的桶在数据库中的页偏移量
		// @Return:		(void)
		// @Function:	通过传入的DBController实例，根据页偏移量读取相应的哈希索引页到缓冲区中，会将原来HashIndex对象中的信息覆盖且不会自动保存原来信息
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

		// @Parameters:	dbController 传入的DBController对象，用于读取页
		// @Return:		(void)
		// @Function:	创建新的哈希索引页，并将其载入到缓冲区中
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

		// @Parameters:	dbController 传入的DBController对象，用于读取页，pageNumber 要读取的页偏移量
		// @Return:		(void)
		// @Function:	根据页偏移量读取相应的哈希索引页到缓冲区中，会将原来HashIndex对象中的信息覆盖且不会自动保存原来信息
		void setHashIndex(DBController &dbController, int pageNumber)
		{
			_hashIndex = &dbController.GetBufferMappingToPage(DatabaseIndexFile, pageNumber);
			_hashIndexBuffer = _hashIndex->getBuffer();
			_capacity = _hashIndex->getCapacity();
			_currentNum = (int*)_hashIndexBuffer;
			_maxNum = (_capacity - sizeof(int)) / sizeof(indexNode);
		}

		// @Parameters:	toInsert 表示待插入的哈希索引节点信息
		// @Return:		bool值，若插入成功返回true，否则返回false
		// @Function:	根据传入的节点信息创建新的索引节点
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

		// @Parameters:	待返回的索引节点在该页中的编号(从1开始)
		// @Return:		返回所要求的索引节点
		// @Function:	根据输入编号返回相应的索引节点头指针
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


