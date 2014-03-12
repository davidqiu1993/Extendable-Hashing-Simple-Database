#ifndef _BUCKET_H_
#define _BUCKET_H_

#include "General.h"

namespace exth
{
	//指示插入到当前缓冲区页的返回状态
	enum InsertState 
	{
		SUCCEED = 0,
		NEED_SPLIT = 1
	};

	//桶内槽定义
	class Slot
	{
	private:
		int _offset;	//该槽所指向记录的储存起始偏移量
		int _rID;		//记录的orderKey值
		int _rLen;		//记录二进制形式长度

	public:
		Slot()
		{
			_offset = -1;
			_rID = 0;
			_rLen = 0;
		}
		Slot(int offset, int rID, int rLen)
		{
			_offset = offset;
			_rID = rID;
			_rLen = rLen;
		}
		
		int getOffset()
		{
			return _offset;
		}
		void setOffset(int offset)
		{
			_offset = offset;
		}

		int getRID()
		{
			return _rID;
		}
		int getRLen()
		{
			return _rLen;
		}
	};

	class Bucket
	{
	private:

		// @Parameters:	toSlotPtr 指向起始指针的指针，因为要修改起始指针，所以传入指针的指针；endPtr 表示终止指针
		// @Return:		(void)
		// @Function:	根据起始与终止指针，寻找出第一个空槽
		void nextToSlotPtr(char** toSlotPtr, char* endPtr)
		{
			while(*toSlotPtr != endPtr)
			{
				int offset = *((int*)(*toSlotPtr));
				if(offset != -1)
					(*toSlotPtr) += sizeof(Slot);
				else
					break;
			}
		}

		// @Parameters:	fromSlotPtr 指向起始指针的指针，因为要修改起始指针，所以传入指针的指针；endPtr 表示终止指针
		// @Return:		(void)
		// @Function:	根据起始与终止指针，寻找出第一个之前有空槽的非空槽
		void nextFromSlotPtr(char **fromSlotPtr, char *endPtr)
		{
			while(*fromSlotPtr != endPtr)
			{
				int offset = *((int*)(*fromSlotPtr));
				if(offset == -1)
					(*fromSlotPtr) += sizeof(Slot);
				else
					break;
			}
		}

		// @Parameters:	(void)
		// @Return:		(void)
		// @Function:	将所有槽重新排列，把所有空槽覆盖，但并不修改每个非空槽所指向记录的位置
		void resortSlot()
		{
			char *startPtr = (char*)_bucketBuffer + *_slotStart;
			char *endPtr = (char*)_bucketBuffer + *_slotStart + (*_slotNumber) * sizeof(Slot);
			Slot temp;

			while(startPtr != endPtr)				//统计有多少非空槽
			{
				temp = *((Slot*)startPtr);
				if(temp.getOffset() == -1)
				{
					(*_slotNumber)--;
					*_freeStart -= sizeof(Slot);
					*_freeLen += sizeof(Slot);
				}

				startPtr += sizeof(Slot);
			}

			char *toSlotPtr = (char*)_bucketBuffer + *_slotStart;
			nextToSlotPtr(&toSlotPtr, endPtr);

			char *fromSlotPtr = toSlotPtr;
			nextFromSlotPtr(&fromSlotPtr, endPtr);

			while(fromSlotPtr != endPtr)			//清理空槽
			{
				memcpy(toSlotPtr, fromSlotPtr, sizeof(Slot));
				*((int*)fromSlotPtr) = -1;

				nextToSlotPtr(&toSlotPtr, endPtr);
				nextFromSlotPtr(&fromSlotPtr, endPtr);
			}
		}

		// @Parameters:	(void)
		// @Return:		(void)
		// @Function:	根据重新排列的槽，将其对应的记录重新排列，将记录间的空位覆盖
		void resortRecord()
		{
			char *endSlotPtr = (char*)_bucketBuffer + *_slotStart + sizeof(Slot)*(*_slotNumber);
			char *insertPtr = (char*)_bucketBuffer + _capacity;
			int currentOffset = _capacity;

			for(char* iptr = (char*)_bucketBuffer + *_slotStart;
				iptr!=endSlotPtr; iptr+=sizeof(Slot))
			{
				Slot temp = *((Slot*)iptr);
				currentOffset -= temp.getRLen();
				insertPtr -= temp.getRLen();

				memmove(insertPtr, (char*)_bucketBuffer + temp.getOffset(), temp.getRLen());
				((Slot*)iptr)->setOffset(currentOffset);
			}

			(*_freeLen) = currentOffset - (*_freeStart);
			Save();
		}


		// @Parameters:	(void)
		// @Return:		(void)
		// @Function:	从Bucket对象所指向的缓冲区页中读取相关信息
		void readBucket()
		{
			_bucketBuffer = _bucket->getBuffer();
			_capacity = _bucket->getCapacity();

			_bucketID = (int*)_bucketBuffer;
			_bucketAddress = _bucketID + 1;
			_localDepth = _bucketAddress + 1;
			_slotStart = _localDepth + 1;
			_slotNumber = _slotStart + 1;
			_freeStart = _slotNumber + 1;
			_freeLen = _freeStart + 1;
		}

		void setBucketAddress(const int &bucketAddress)
		{
			*_bucketAddress = bucketAddress;
		}



//----------------------------------------------------------------------
//属性
//----------------------------------------------------------------------



		//无需存储在桶内
		MemoryBuffer *_bucket;
		void *_bucketBuffer;	//用于记录桶在内存中的对应缓冲区起始地址
		int _capacity;			//每个桶容量(字节为单位)


		//指针所指向的属性按顺序存储在每个桶内
		int *_bucketID;		//该桶的哈希索引值(相对于局部深度)
		int *_bucketAddress;//该桶在文件中储存的偏移值，一个页偏移值为1，偏移量从0开始
		int *_localDepth;
		int *_slotStart;	//记录槽组起始位置的字节偏移量
		int *_slotNumber;	//所有槽的总个数，包括空槽
		int *_freeStart;	//记录空闲空间起始位置的字节偏移量
		int *_freeLen;		//空闲空间总字节数

		

	public:
		// @Parameters:	dbController 传入的DBController实例，用于对数据库内容进行操作
		// @Return:		(void)
		// @Function:	通过传入的DBController实例，创建新的bucket对象
		Bucket(DBController &dbController){
			setBucket(dbController);
		}

		// @Parameters:	dbController 传入的DBController实例，用于对数据库内容进行操作，pageNumber 要读取的桶在数据库中的页偏移量
		// @Return:		(void)
		// @Function:	根据传入的页偏移量(从0开始)，通过传入的DBController实例，读取相应的桶页并生成相应Bucket对象
		Bucket(DBController &dbController, int pageNumber){
			setBucket(dbController, pageNumber);
		}

		// @Parameters: origin 需要复制的实例
		// @Return:		(void)
		// @Function:	根据传入的实例创建一个新的Bucket对象
		Bucket(const Bucket& origin)
		{
			this->_bucket = origin._bucket;
			this->_bucketBuffer = origin._bucketBuffer;
			this->_capacity = origin._capacity;

			this->_bucketID = origin._bucketID;
			this->_bucketAddress = origin._bucketAddress;
			this->_localDepth = origin._localDepth;
			this->_slotStart = origin._slotStart;
			this->_slotNumber = origin._slotNumber;
			this->_freeStart = origin._freeStart;
			this->_freeLen = origin._freeLen;
		}

		// @Parameters: origin 赋值操作的来源
		// @Return:		赋值的左操作数
		// @Function:	赋值操作重载
		Bucket& operator =(const Bucket& origin)
		{
			this->_bucket = origin._bucket;
			this->_bucketBuffer = origin._bucketBuffer;
			this->_capacity = origin._capacity;

			this->_bucketID = origin._bucketID;
			this->_bucketAddress = origin._bucketAddress;
			this->_localDepth = origin._localDepth;
			this->_slotStart = origin._slotStart;
			this->_slotNumber = origin._slotNumber;
			this->_freeStart = origin._freeStart;
			this->_freeLen = origin._freeLen;

			return (*this);
		}



		// @Parameters:	dbController 传入的DBController对象，用于读取页，pageNumber 要读取的页偏移量
		// @Return:		(void)
		// @Function:	根据页偏移量读取相应的桶到缓冲区中，会将原来Bucket对象中的信息覆盖且不会自动保存原来信息
		void setBucket(DBController &dbController)
		{
			int pageNumber = dbController.AppendNewPage(DatabaseContentFile);
			_bucket = &dbController.GetBufferMappingToPage(DatabaseContentFile, pageNumber);
			readBucket();
		}

		// @Parameters:	dbController 传入的DBController对象，用于读取页，pageNumber 要读取的页偏移量
		// @Return:		(void)
		// @Function:	根据页偏移量读取相应的桶到缓冲区中，会将原来Bucket对象中的信息覆盖且不会自动保存原来信息
		void setBucket(DBController &dbController, int pageNumber)
		{
			_bucket = &dbController.GetBufferMappingToPage(DatabaseContentFile, pageNumber);
			readBucket();
		}

		// @Parameters:	bucketID 桶编号，指的是桶的哈希值，localDepth 桶的局部深度
		// @Return:		(void)
		// @Function:	根据传入的参数为各属性赋值
		void initialBucket(int bucketID ,int localDepth)
		{
			int offset = sizeof(int)*7;

			setBucketID(bucketID);
			setBucketAddress(_bucket->getPageNumber());
			setlocalDepth(localDepth);
			setslotStart(offset);
			setslotNumber(0);
			setFreeStart(offset);
			setFreeLen(_capacity - offset);
		}

		// @Parameters:	toInsert 传入带插入记录
		// @Return:		返回插入状态信息，分别为需要分裂和插入成功两种情况
		// @Function:	插入一条记录到当前页，并返回相应的成功或失败信息
		InsertState insert(Record toInsert)
		{
			int length = 0;		//用来表示记录长度
			void *recordPtr = toInsert.GetBytes(length);

			if ((length + sizeof(Slot)) > (*_freeLen))
				return NEED_SPLIT;
			else
			{
				//插入记录
				(*_freeLen) -= length;
				char *insertPtr = (char*)_bucketBuffer + (*_freeStart) + (*_freeLen);	
				memcpy(insertPtr, recordPtr, length);

				//插入对应槽
				char *slotPtr = (char*)_bucketBuffer + (*_slotStart) + (*_slotNumber) * sizeof(Slot);
				Slot newSlot = Slot((*_freeStart) + (*_freeLen), toInsert.getOrderKey(), length);
				memcpy(slotPtr, &newSlot, sizeof(Slot));

				(*_slotNumber)++;
				(*_freeStart) += sizeof(Slot);
				(*_freeLen) -= sizeof(Slot);

				return SUCCEED;
			}	
		}

		// @Parameters:	(void)
		// @Return:		(void)
		// @Function:	重新组织桶内的内容，将槽以及记录内部的空闲空间全部覆盖
		void resort()
		{
			resortSlot();
			resortRecord();
		}

		// @Parameters:	index 表示要移除第几条记录,从1开始
		// @Return:		返回要移除的记录，如果移除不成功则返回orderkey为-1的记录
		// @Function:	根据提示移除相关记录，并返回相应的成功或失败信息
		Record remove(int index)
		{
			Record toReturn = Record();

			if(index<=0 || index>(*_slotNumber))		//偏移量越界
			{
				toReturn.setOrderKey(-1);
				return toReturn;
			}
			else
			{
				Slot temp = Slot();
				char *slotPtr = (char*)_bucketBuffer + *_slotStart + (index-1)*sizeof(Slot);
				memcpy(&temp, slotPtr, sizeof(Slot));

				if(temp.getOffset() == -1)			//该槽无效
				{
					toReturn.setOrderKey(-1);
					return toReturn;
				}
				else								//操作合法
				{
					char *recordPtr = (char*)_bucketBuffer + temp.getOffset();
					toReturn = Record::Parse((void*)recordPtr, temp.getRLen());
					*((int*)slotPtr) = -1;
					return toReturn;
				}
			}
		}

		// @Parameters:	index 表示要返回第几条记录,从1开始
		// @Return:		返回记录，如果没有相关记录则返回orderkey为-1的记录
		// @Function:	根据提示返回相关记录，并返回相应的成功或失败信息
		Record getRecord(int index)
		{
			Record toReturn = Record();

			if(index<=0 || index>(*_slotNumber))		//偏移量越界
			{
				toReturn.setOrderKey(-1);
				return toReturn;
			}
			else
			{
				Slot temp = Slot();
				char *slotPtr = (char*)_bucketBuffer + *_slotStart + (index-1)*sizeof(Slot);
				memcpy(&temp, slotPtr, sizeof(Slot));

				if(temp.getOffset() == -1)			//该槽无效
				{
					toReturn.setOrderKey(-1);
					return toReturn;
				}
				else								//操作合法
				{
					char *recordPtr = (char*)_bucketBuffer + temp.getOffset();
					return Record::Parse((void*)recordPtr, temp.getRLen());
				}
			}
		}

		// @Parameters:	(void)
		// @Return:		A bool indicating whether the buffer is saved to the corresponding disk page successfully
		// @Function:	Save the buffer to the corresponding disk page indicated by the page number.
		void Save()
		{
			_bucket->Save();
		}

		// @Parameters:	(void)
		// @Return:		(void)
		// @Function:	Pin one pin on this memory buffer.
		void Pin()
		{
			_bucket->Pin();
		}

		// @Parameters:	(void)
		// @Return:		(void)
		// @Function:	Unpin one pin off this memory buffer.
		void Unpin()
		{
			_bucket->Unpin();
		}


		// @Parameters:	(void)
		// @Return:		(void)
		// @Function:	返回该Bucket对象所代表的缓冲区起始指针
		void* getBucketBufferPtr()
		{
			return _bucketBuffer;
		}

		const int& getBucketID()
		{
			return *_bucketID;
		}
		void setBucketID(const int &bucketID)
		{
			*_bucketID = bucketID;
		}

		const int& getBucketAddress()
		{
			/*			return *_bucketAddress;*/
			return _bucket->getPageNumber();
		}

		const int& getLocalDepth()
		{
			return *_localDepth;
		}
		void setlocalDepth(const int &localDepth)
		{
			*_localDepth = localDepth;
		}

		const int& getslotStart()
		{
			return *_slotStart;
		}
		void setslotStart(const int &slotStart)
		{
			*_slotStart = slotStart;
		}

		const int& getslotNumber()
		{
			return *_slotNumber;
		}
		void setslotNumber(const int &slotNumber)
		{
			*_slotNumber = slotNumber;
		}

		const int& getFreeStart()
		{
			return *_freeStart;
		}
		void setFreeStart(const int &freeStart)
		{
			*_freeStart = freeStart;
		}

		const int& getFreeLen()
		{
			return *_freeLen;
		}
		void setFreeLen(const int &freeLen)
		{
			*_freeLen = freeLen;
		}

		const int& getCapacity()
		{
			return _capacity;
		}
		void setCapacity(const int &capacity)
		{
			_capacity = capacity;
		}
	};
};
#endif	//_BUCKET_H_
