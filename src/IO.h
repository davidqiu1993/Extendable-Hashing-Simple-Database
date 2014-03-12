#ifndef _IO_H_
#define _IO_H_

#include "General.h"

namespace exth
{
	// @Summary:	The enum indicating the state of a file connection.
	enum FileState
	{
		NoFile = -1,
		FileClosed = 0,
		FileOpened = 1
	};

	// @Summary:	The enum indicating which database file to operate with.
	enum DatabaseFile
	{
		DatabaseContentFile = 0,
		DatabaseIndexFile = 1
	};

	// @Summary:	The file reader for tbl files.
	class TblFileReader
	{
	private:
		string _filePath;
		FileState _fileState;
		FILE* _streamPointer;

		// @Notice:		Restricted copy constructor.
		TblFileReader(const TblFileReader& origin){;}

	public:
		// @Parameters:	(void)
		// @Return:		(void)
		// @Function:	Create an instant of TblFileReader without initialization.
		TblFileReader()
		{
			_fileState = FileState::NoFile;
			_streamPointer = NULL;
		}

		// @Parameters: filePath: The path of the tbl file
		// @Return:		(void)
		// @Function:	Create an instant of TblFileReader with initialization of the file path.
		TblFileReader(const string filePath) 
		{
			_filePath = filePath;
			_fileState = FileState::FileClosed;
			_streamPointer = NULL;
		}

		// @Attribute: FilePath
		string getFilePath()
		{
			return _filePath;
		}
		void setFilePath(const string value)
		{
			if(_fileState == FileState::FileOpened) throw runtime_error("exth::TblFileReader::setFilePath(string): The file is opened, thus the path cannot be reset at the moment.");
			if(value == "") throw invalid_argument("exth::TblFileReader::setFilePath(string): The file path cannot be empty.");

			_filePath = value;
			_fileState = FileState::FileClosed;
		}

		// @Attribute: FileState [ReadOnly]
		FileState getFileState()
		{
			return _fileState;
		}

		// @Parameters:	(void)
		// @Return:		A bool indicating whether the tbl file is opened successfully
		// @Function:	Open a tbl file indicated by the file path.
		bool Open()
		{
			if(_fileState == FileState::NoFile) throw domain_error("exth::TblFileReader::Open(): The file path is not specific.");

			_streamPointer = fopen(_filePath.c_str(), "r");
			if(_streamPointer == NULL) throw runtime_error("exth::TblFileReader::Open(): The file specific by the path does not exists.");
			_fileState = FileState::FileOpened;
		}

		// @Parameters:	(void)
		// @Return:		(void)
		// @Function:	Close the tbl file.
		void Close()
		{
			fclose(_streamPointer);
			_fileState = FileState::FileClosed;
		}

		// @Parameters:	(void)
		// @Return:		A Record indicating the current read record from the tbl file, if it is the end then the orderkey of record is -1.
		// @Function:	Read a record from the opened tbl file and then point to the next record.
		Record Read()
		{
			char str[256];

			if(fgets(str, 255, _streamPointer)!=NULL) return Record::Parse(string(str));
			else
			{
				Record record;
				record.setOrderKey(-1);
				return record;
			}
		}

		// @Parameters:	(void)
		// @Return:		(void)
		// @Function:	Release all resources.
		~TblFileReader()
		{
			;
		}
	};

	class DBController;
	// @Summary:	The entity as a memory buffer.
	class MemoryBuffer
	{
		friend class DBController;

#ifdef DEBUG
	public:
#endif // DEBUG
#ifndef DEBUG
	private:
#endif // DEBUG
		static const int _capacity = 8192;	// capacity = 8K = 8 * 1024
		DatabaseFile _dbfile;				// database file to operated with
		int _pageNumber;					// -1 indicates unused
		void* _buffer;						// memory buffer pointer
		DBController* _container;			// the one that contains the information of this memory buffer
		int _pinCount;						// the count of pins on this memory buffer
		bool _clockScanned;					// indicates if it has been scanned in the LRU clockwise

		// @Parameters:	creater: the DBController that creates this MemoryBuffer
		// @Return:		(void)
		// @Function:	Create an instant of MemoryBuffer with initialization of the buffer allocation.
		MemoryBuffer(DBController* creater);

		// @Parameters:	origin: the MemoryBuffer copied from
		// @Return:		(void)
		// @Function:	Copy constructor. Fully copy including the container and clock scan state.
		MemoryBuffer(const MemoryBuffer& origin);

		// @Parameters:	origin: the MemoryBuffer copied from
		// @Return:		(void)
		// @Function:	Assignment operator override. Fully copy including the container and clock scan state.
		MemoryBuffer& operator =(const MemoryBuffer& origin);

		// @Parameters:	(void)
		// @Return:		(void)
		// @Function:	Reinitialize this MemoryBuffer.
		void Clear(DBController* container);

		// @Attribute: ClockScanned
		bool getClockScanned()
		{
			return _clockScanned;
		}
		void setClockScanned(bool value)
		{
			_clockScanned = value;
		}
		void ClockScan()
		{
			_clockScanned = true;
		}

		// @Attribute: PinCount [ReadOnly]
		int getPinCount()
		{
			return _pinCount;
		}

	public:
		// @Parameters:	(void)
		// @Return:		(void)
		// @Function:	Save the buffer to the corresponding disk page indicated by the page number.
		// @Exceptions:	runtime_error: if the container is not specific.
		//				runtime_error: if the database files are not opened.
		//				runtime_error: if the page is not initialized. (PageNumber < 0)
		//				runtime_error: if the buffer is not initialize (allocated with memory block).
		//				runtime_error: if the database file is not specific.
		void Save();

		// @Parameters:	(void)
		// @Return:		A void* indicating the starting address of the 8K memory buffer
		// @Function:	Get the starting address of the memory buffer.
		void* getBuffer()
		{
			_clockScanned = false;
			return _buffer;
		}

		// @Attribute: DBFile
		DatabaseFile getDBFile()
		{
			return _dbfile;
		}
		void setDBFile(const DatabaseFile value)
		{
			_clockScanned = false;
			_dbfile = value;
		}

		// @Attribute: PageNumber
		int getPageNumber()
		{
			return _pageNumber;
		}
		void setPageNumber(const int value)
		{
			_clockScanned = false;
			_pageNumber = value;
		}

		// @Attribute: Capacity
		static int getCapacity()
		{
			return _capacity;
		}

		// @Attribute: Container
		DBController& getContainer();

		// @Parameters:	(void)
		// @Return:		(void)
		// @Function:	Pin one pin on this memory buffer.
		void Pin()
		{
			_clockScanned = false;
			++(_pinCount);
		}

		// @Parameters:	(void)
		// @Return:		(void)
		// @Function:	Unpin one pin off this memory buffer.
		// @Exceptions:	underflow_error: if the current pin count is 0.
		void Unpin()
		{
			if(_pinCount == 0) throw underflow_error("exth::MemoryBuffer::Unpin(): The pin count is 0 that cannot be unpinned.");
			--(_pinCount);
		}

		// @Parameters:	(void)
		// @Return:		(void)
		// @Function:	Release all the resources.
		~MemoryBuffer()
		{
			free(_buffer);
		}
	};

	// @Summary:	The controller of a database and the corresponding database file.
	class DBController
	{
		friend class MemoryBuffer;

#ifdef DEBUG
	public:
#endif // DEBUG
#ifndef DEBUG
	private:
#endif // DEBUG
		string _contentFilePath;		// database content file path
		FILE* _pContentFile;			// database content file
		string _indexFilePath;			// database index file path
		FILE* _pIndexFile;				// database index file
		FileState _dbState;				// database connection state
		int _capacity;					// the capacity of this container, 8 or 128.
		MemoryBuffer** _bufferArray;	// the buffer array [in LRU]
		int _clockArrow;				// indicates the position of the LRU clock

		// @Parameters:	(void)
		// @Return:		A MemoryBuffer* indicating the memory buffer to be swapped.
		//				NULL is returned if there is no available buffer.
		// @Function:	Find a memory buffer for swapping and reinitialize it.
		MemoryBuffer* _findSwappingBuffer()
		{
			int i = 0; // scan times
			int max_scan_times = _capacity * 2; // maximum scan times

			while (!(_bufferArray[_clockArrow]->getPageNumber() < 0 || 
				(_bufferArray[_clockArrow]->getClockScanned() && _bufferArray[_clockArrow]->getPinCount() == 0)))
			{
				_bufferArray[_clockArrow]->ClockScan();
				_clockArrow = (_clockArrow + 1) % _capacity;
				++i;
				if(i>max_scan_times) return NULL;
			}

			_bufferArray[_clockArrow]->Clear(this);
			return _bufferArray[_clockArrow];
		}

		// @Parameters:	dbfile: the database file to operate with
		//				pageNumber: the page number in the corresponding disk file
		// @Return:		A MemoryBuffer* indicating the buffer found in the _bufferArray.
		//				NULL is returned if the buffer is not found.
		// @Function:	Find the corresponding MemoryBuffer in the _bufferArray.
		MemoryBuffer* _findBufferInBufferArray(DatabaseFile dbfile, int pageNumber)
		{
			for(int i=0; i<_capacity; ++i)
			{
				if(_bufferArray[i]->getDBFile() == dbfile && _bufferArray[i]->getPageNumber() == pageNumber) return _bufferArray[i];
			}
			return NULL;
		}

		// @Attribute:	ContentFile [ReadOnly]
		// @Notice:		If the file is not opened, then NULL is returned.
		FILE* getContentFile()
		{
			return _pContentFile;
		}

		// @Attribute:	IndexFile [ReadOnly]
		// @Notice:		If the file is not opened, then NULL is returned.
		FILE* getIndexFile()
		{
			return _pIndexFile;
		}

		// @BAN: Invalid methods.
		DBController(const DBController& origin) {;}
		DBController& operator =(const DBController& origin) {return *this;}

	public:
		// @Parameters:	capacity: the number of memory buffers it holds
		// @Return:		(void)
		// @Function:	Create an instant of DBController.
		// @Exceptions:	out_of_range: if the capacity is less than 1.
		DBController(const int capacity)
		{
			if(capacity < 1) throw out_of_range("exth::DBController::DBController(const int): The capacity must be greater or equal to 1.");

			_capacity = capacity;
			_bufferArray = new MemoryBuffer*[_capacity];
			for(int i=0; i<_capacity; ++i)
			{
				_bufferArray[i] = new MemoryBuffer(this);
			}

			_pContentFile = NULL;
			_pIndexFile = NULL;
			_dbState = FileState::NoFile;
			_clockArrow = 0;
		}

		// @Parameters:	capacity: the number of memory buffers it holds
		//				dbContentFilePath: the path of the database content file
		//				dbIndexFilePath: the path of the database index file
		// @Return:		(void)
		// @Function:	Create an instant of DBController with initialization of the file path.
		// @Exceptions:	out_of_range: if the capacity is less than 1.
		DBController(const int capacity, const string dbContentFilePath, const string dbIndexFilePath)
		{
			if(capacity < 1) throw out_of_range("exth::DBController::DBController(const int): The capacity must be greater or equal to 1.");

			_contentFilePath = dbContentFilePath;
			_indexFilePath = dbIndexFilePath;

			_capacity = capacity;
			_bufferArray = new MemoryBuffer*[_capacity];
			for(int i=0; i<_capacity; ++i)
			{
				_bufferArray[i] = new MemoryBuffer(this);
			}

			_pContentFile = NULL;
			_pIndexFile = NULL;
			_dbState = FileState::FileClosed;
			_clockArrow = 0;
		}

		// @Attribute:	ContentFilePath
		// @Exceptions:	runtime_error: if the file path is reset while database files opened.
		// @Notice:		The database file must be closed or not existing before resetting the file path.
		string getContentFilePath()
		{
			return _contentFilePath;
		}
		void setContentFilePath(const string& value)
		{
			if(_dbState == FileState::FileOpened) throw runtime_error("exth::DBController::setContentFilePath(const string&): The database files are opened.");
			_contentFilePath = value;
			_dbState = FileState::FileClosed;
		}

		// @Attribute:	IndexFilePath
		// @Exceptions:	runtime_error: if the file path is reset while database files opened.
		// @Notice:		The database file must be closed or not existing before resetting the file path.
		string getIndexFilePath()
		{
			return _indexFilePath;
		}
		void setIndexFilePath(const string& value)
		{
			if(_dbState == FileState::FileOpened) throw runtime_error("exth::DBController::setContentFilePath(const string&): The database files are opened.");
			_indexFilePath = value;
			_dbState = FileState::FileClosed;
		}

		// @Attribute:	DBState [ReadOnly]
		// @Notice:		The file state indicates the states of both content and index files.
		FileState getDBState()
		{
			return _dbState;
		}

		// @Parameters:	(void)
		// @Return:		A bool indicating whether DB files are opened successfully
		// @Function:	Open DB files indicated by the file paths.
		// @Exceptions:	runtime_error: if the file paths are not specific.
		//				runtime_error: if the database files are opened.
		//				runtime_error: if any database file is occupied or does not exists.
		bool Open()
		{
			if(_dbState == FileState::NoFile) throw runtime_error("exth::DBController::Open(): The file paths are not specific.");
			if(_dbState == FileState::FileOpened) throw runtime_error("exth::DBController::Open(): The database files are opened.");
			try
			{
				_pContentFile = fopen(_contentFilePath.c_str(), "rb+");
				_pIndexFile = fopen(_indexFilePath.c_str(), "rb+");
				_dbState = FileState::FileOpened;
			}
			catch(...)
			{
				fclose(_pContentFile);
				fclose(_pIndexFile);
				_pContentFile = NULL;
				_pIndexFile = NULL;
				_dbState = FileState::FileClosed;
				throw runtime_error("exth::DBController::Open(): The database files are occupied or do not exits.");
			}
		}

		// @Parameters:	(void)
		// @Return:		(void)
		// @Function:	Close the DB files.
		void Close()
		{
			//if(_dbState != FileState::FileOpened) throw runtime_error("exth::DBController::Close(): The database files are not opened.");

			fclose(_pContentFile);
			fclose(_pIndexFile);
			_pContentFile = NULL;
			_pIndexFile = NULL;
			_dbState = FileState::FileClosed;
		}

		// @Parameters:	dbfile: the database file to operate with
		//				pageNumber: the number of the page requested in the database file
		// @Return:		A MemoryBuffer& indicating the memory buffer that maps to the requested page
		// @Function:	Get a memory buffer mapping to a disk page indicated by the DB file and the page number.
		// @Exceptions:	runtime_error: if the database files are not opened.
		//				out_of_range: if the pageNumber is less than 0.
		//				runtime_error: if the buffer pool is full.
		//				runtime_error: if the database files are not specific.
		//				out_of_range: if the request pageNumber exceeds the exact maximum number of pages.
		MemoryBuffer& GetBufferMappingToPage(DatabaseFile dbfile, int pageNumber)
		{
			if(_dbState != FileState::FileOpened) throw runtime_error("exth::DBController::GetBufferMappingToPage(DatabaseFile, int): The database files are not opened.");
			if(pageNumber<0) throw out_of_range("exth::DBController::GetBufferMappingToPage(DatabaseFile, int): The pageNumber cannot be less than 0.");

			MemoryBuffer* foundBuf = _findBufferInBufferArray(dbfile, pageNumber);
			if(foundBuf != NULL)
			{
				// Buffer found
				foundBuf->setClockScanned(false);
				return (*foundBuf);
			}
			else
			{
				// Buffer not found
				foundBuf = _findSwappingBuffer();
				if(foundBuf == NULL) throw runtime_error("exth::DBController::GetBufferMappingToPage(DatabaseFile, int): The buffer pool is full, thus no more page can be loaded.");

				// Available buffer found: foundBuf == empty buffer
				try
				{
					int fileSize, pageCount;

#ifdef TEST_STATISTIC
					int ts_begin_time, ts_end_time;
#endif // TEST_STATISTIC
					switch (dbfile)
					{
					case exth::DatabaseContentFile:
						fseek(_pContentFile, 0, SEEK_END);
						fileSize = ftell(_pContentFile);
						pageCount = (int)(fileSize / MemoryBuffer::getCapacity());
						if(pageNumber >= pageCount) throw out_of_range("exth::DBController::GetBufferMappingToPage(DatabaseFile, int): The PageNumber exceeds the maximum page number.");
						fseek(_pContentFile, pageNumber * MemoryBuffer::getCapacity(), SEEK_SET);
#ifdef TEST_STATISTIC
						++ts_input_count;
						ts_begin_time = clock(); 
#endif // TEST_STATISTIC
						fread(foundBuf->getBuffer(), 1, MemoryBuffer::getCapacity(), _pContentFile);
#ifdef TEST_STATISTIC
						ts_end_time = clock();
						ts_input_time += ts_end_time - ts_begin_time;
#endif // TEST_STATISTIC
						foundBuf->setDBFile(DatabaseFile::DatabaseContentFile);
						break;
					case exth::DatabaseIndexFile:
						fseek(_pIndexFile, 0, SEEK_END);
						fileSize = ftell(_pIndexFile);
						pageCount = (int)(fileSize / MemoryBuffer::getCapacity());
						if(pageNumber >= pageCount) throw out_of_range("exth::DBController::GetBufferMappingToPage(DatabaseFile, int): The PageNumber exceeds the maximum page number.");
						fseek(_pIndexFile, pageNumber * MemoryBuffer::getCapacity(), SEEK_SET);
#ifdef TEST_STATISTIC
						++ts_input_count;
						ts_begin_time = clock(); 
#endif // TEST_STATISTIC
						fread(foundBuf->getBuffer(), 1, MemoryBuffer::getCapacity(), _pIndexFile);
#ifdef TEST_STATISTIC
						ts_end_time = clock();
						ts_input_time += ts_end_time - ts_begin_time;
#endif // TEST_STATISTIC
						foundBuf->setDBFile(DatabaseFile::DatabaseIndexFile);
						break;
					default:
						throw runtime_error("exth::DBController::GetBufferMappingToPage(DatabaseFile, int): The database file is not specific.");
						break;
					}
				}
				catch(...)
				{
					throw out_of_range("exth::DBController::GetBufferMappingToPage(DatabaseFile, int): The request pageNumber exceeds the exact maximum number of pages.");
				}

				foundBuf->setPageNumber(pageNumber);
				foundBuf->setClockScanned(false);
				return (*foundBuf);
			}
		}

		// @Parameters:	dbfile: the database file to operate with
		//				firstPage: the first page to swap
		//				secondPage: the second page to swap
		// @Return:		(void)
		// @Function:	Swap two pages in a database file.
		// @Exceptions:	runtime_error: if the database files are not opened.
		//				out_of_range: if the pageNumber is less than 0.
		//				runtime_error: if the buffer pool is full.
		//				runtime_error: if the database files are not specific.
		//				out_of_range: if the request pageNumber exceeds the exact maximum number of pages.
		void SwapPage(DatabaseFile dbfile, int firstPage, int secondPage)
		{
			if(_dbState != FileState::FileOpened) throw runtime_error("exth::DBController::SwapPage(DatabaseFile, int, int): The database files are not opened.");
			if(firstPage<0 || secondPage<0) throw out_of_range("exth::DBController::SwapPage(DatabaseFile, int, int): Any page number of the first and second pages is less than 0.");

			MemoryBuffer* buf1 = _findSwappingBuffer();
			if(buf1 == NULL) throw runtime_error("exth::DBController::SwapPage(DatabaseFile, int, int): The buffer pool is full.");
			buf1->Pin();
			MemoryBuffer* buf2 = _findSwappingBuffer();
			if(buf2 == NULL)
			{
				buf1->Clear(this);
				throw runtime_error("exth::DBController::SwapPage(DatabaseFile, int, int): The buffer pool is full");
			}
			buf2->Pin();

			// Memory buffers allocated successfully
			try
			{
				int fileSize, pageCount;

#ifdef TEST_STATISTIC
				int ts_begin_time, ts_end_time;
#endif // TEST_STATISTIC

				switch (dbfile)
				{
				case exth::DatabaseContentFile:
					fseek(_pContentFile, 0, SEEK_END);
					fileSize = ftell(_pContentFile);
					pageCount = (int)(fileSize / MemoryBuffer::getCapacity());
					if(firstPage>=pageCount || secondPage>=pageCount) throw out_of_range("exth::DBController::SwapPage(DatabaseFile, int, int): The request PageNumber exceeds the maximum page number.");
#ifdef TEST_STATISTIC
					ts_input_count += 2;
					ts_begin_time = clock(); 
#endif // TEST_STATISTIC
					fseek(_pContentFile, firstPage * MemoryBuffer::getCapacity(), SEEK_SET); // read the first page
					fread(buf1->getBuffer(), MemoryBuffer::getCapacity(), 1, _pContentFile);
					fseek(_pContentFile, secondPage * MemoryBuffer::getCapacity(), SEEK_SET); // read the second page
					fread(buf2->getBuffer(), MemoryBuffer::getCapacity(), 1, _pContentFile);
#ifdef TEST_STATISTIC
					ts_end_time = clock();
					ts_input_time += ts_end_time - ts_begin_time;
#endif // TEST_STATISTIC
#ifdef TEST_STATISTIC
					ts_output_count += 2;
					ts_begin_time = clock(); 
#endif // TEST_STATISTIC
					fseek(_pContentFile, secondPage * MemoryBuffer::getCapacity(), SEEK_SET); // second' <- first
					fwrite(buf1->getBuffer(), MemoryBuffer::getCapacity(), 1, _pContentFile);
					fseek(_pContentFile, firstPage * MemoryBuffer::getCapacity(), SEEK_SET); // first' <- second
					fwrite(buf2->getBuffer(), MemoryBuffer::getCapacity(), 1, _pContentFile);
#ifdef TEST_STATISTIC
					ts_end_time = clock();
					ts_output_time += ts_end_time - ts_begin_time;
#endif // TEST_STATISTIC
					break;
				case exth::DatabaseIndexFile:
					fseek(_pIndexFile, 0, SEEK_END);
					fileSize = ftell(_pIndexFile);
					pageCount = (int)(fileSize / MemoryBuffer::getCapacity());
					if(firstPage>=pageCount || secondPage>=pageCount) throw out_of_range("exth::DBController::SwapPage(DatabaseFile, int, int): The request PageNumber exceeds the maximum page number.");
#ifdef TEST_STATISTIC
					ts_input_count += 2;
					ts_begin_time = clock(); 
#endif // TEST_STATISTIC
					fseek(_pIndexFile, firstPage * MemoryBuffer::getCapacity(), SEEK_SET); // read the first page
					fread(buf1->getBuffer(), MemoryBuffer::getCapacity(), 1, _pIndexFile);
					fseek(_pIndexFile, secondPage * MemoryBuffer::getCapacity(), SEEK_SET); // read the second page
					fread(buf2->getBuffer(), MemoryBuffer::getCapacity(), 1, _pIndexFile);
#ifdef TEST_STATISTIC
					ts_end_time = clock();
					ts_input_time += ts_end_time - ts_begin_time;
#endif // TEST_STATISTIC
#ifdef TEST_STATISTIC
					ts_output_count += 2;
					ts_begin_time = clock(); 
#endif // TEST_STATISTIC
					fseek(_pIndexFile, secondPage * MemoryBuffer::getCapacity(), SEEK_SET); // second' <- first
					fwrite(buf1->getBuffer(), MemoryBuffer::getCapacity(), 1, _pIndexFile);
					fseek(_pIndexFile, firstPage * MemoryBuffer::getCapacity(), SEEK_SET); // first' <- second
					fwrite(buf2->getBuffer(), MemoryBuffer::getCapacity(), 1, _pIndexFile);
#ifdef TEST_STATISTIC
					ts_end_time = clock();
					ts_output_time += ts_end_time - ts_begin_time;
#endif // TEST_STATISTIC
					break;
				default:
					throw runtime_error("exth::DBController::SwapPage(DatabaseFile, int, int): The database file is not specific.");
					break;
				}
			}
			catch(...)
			{
				throw out_of_range("exth::DBController::SwapPage(DatabaseFile, int, int): Any request pageNumber exceeds the exact maximum number of pages.");
			}

			buf1->Clear(this);
			buf2->Clear(this);
		}

		// @Parameters:	dbfile: the database file to operate with
		// @Return:		An int indicating the page number of the new page
		// @Function:	Append a new page at the end of the database file.
		// @Exceptions:	runtime_error: if the database files are not opened.
		int AppendNewPage(DatabaseFile dbfile)
		{
			if(_dbState != FileState::FileOpened) throw runtime_error("exth::DBController::AppendNewPage(DatabaseFile): The database files are not opened.");

			int fileSize, pageCount;
			char endOfFileSysbol = '\0';

#ifdef TEST_STATISTIC
			int ts_begin_time, ts_end_time;
#endif // TEST_STATISTIC

			switch (dbfile)
			{
			case exth::DatabaseContentFile:
				fseek(_pContentFile, 0, SEEK_END);
				fileSize = ftell(_pContentFile);
				pageCount = (int)(fileSize / MemoryBuffer::getCapacity());
#ifdef TEST_STATISTIC
				++ts_output_count;
				ts_begin_time = clock(); 
#endif // TEST_STATISTIC
				fseek(_pContentFile, pageCount * MemoryBuffer::getCapacity(), SEEK_SET);
				fwrite(_bufferArray[0]->getBuffer(), MemoryBuffer::getCapacity(), 1, _pContentFile);
				fseek(_pContentFile, 0, SEEK_END);
				fwrite(&endOfFileSysbol, sizeof(char), 1, _pContentFile); // insert an '\0' to the end of the file
#ifdef TEST_STATISTIC
				ts_end_time = clock();
				ts_output_time += ts_end_time - ts_begin_time;
#endif // TEST_STATISTIC
				break;
			case exth::DatabaseIndexFile:
				fseek(_pIndexFile, 0, SEEK_END);
				fileSize = ftell(_pIndexFile);
				pageCount = (int)(fileSize / MemoryBuffer::getCapacity());
#ifdef TEST_STATISTIC
				++ts_output_count;
				ts_begin_time = clock(); 
#endif // TEST_STATISTIC
				fseek(_pIndexFile, pageCount * MemoryBuffer::getCapacity(), SEEK_SET);
				fwrite(_bufferArray[0]->getBuffer(), MemoryBuffer::getCapacity(), 1, _pIndexFile);
				fseek(_pIndexFile, 0, SEEK_END);
				fwrite(&endOfFileSysbol, sizeof(char), 1, _pIndexFile); // insert an '\0' to the end of the file
#ifdef TEST_STATISTIC
				ts_end_time = clock();
				ts_output_time += ts_end_time - ts_begin_time;
#endif // TEST_STATISTIC
				break;
			default:
				throw runtime_error("exth::DBController::AppendNewPage(DatabaseFile): The database file is not specific.");
				break;
			}

			return pageCount;
		}

		// @Parameters:	(void)
		// @Return:		(void)
		// @Function:	Release all the resources.
		~DBController()
		{
			if(_pContentFile != NULL) fclose(_pContentFile);
			if(_pIndexFile != NULL) fclose(_pIndexFile);
			for(int i=0; i<_capacity; ++i)
			{
				delete _bufferArray[i];
			}
			delete _bufferArray;
		}
	};

	// @Implementation: MemoryBuffer::MemoryBuffer(DBController*)
	MemoryBuffer::MemoryBuffer(DBController* creater)
	{
		_pageNumber = -1;
		_buffer = malloc(_capacity);
		_container = creater;
		_pinCount = 0;
		_clockScanned = false;
	}

	// @Implementation: MemoryBuffer::MemoryBuffer(const MemoryBuffer&)
	MemoryBuffer::MemoryBuffer(const MemoryBuffer& origin)
	{
		this->_dbfile = origin._dbfile;
		this->_pageNumber = origin._pageNumber;
		this->_buffer = malloc(this->_capacity); // allocate a memory block
		memcpy(this->_buffer, origin._buffer, this->_capacity);
		this->_container = origin._container;
		this->_pinCount = origin._pinCount;
		this->_clockScanned = origin._clockScanned;
	}

	// @Implementation: MemoryBuffer& MemoryBuffer::operator =(const MemoryBuffer&)
	MemoryBuffer& MemoryBuffer::operator =(const MemoryBuffer& origin)
	{
		this->_dbfile = origin._dbfile;
		this->_pageNumber = origin._pageNumber;
		memcpy(this->_buffer, origin._buffer, this->_capacity); // memory block has been allocated by any constructor
		this->_container = origin._container;
		this->_pinCount = origin._pinCount;
		this->_clockScanned = origin._clockScanned;
		return (*this);
	}

	// @Implementation: void MemoryBuffer::Clear(DBController*)
	void MemoryBuffer::Clear(DBController* container)
	{
		_pageNumber = -1;
		_container = container;
		_pinCount = 0;
		_clockScanned = false;
	}

	// @Implementation: void MemoryBuffer::Save()
	void MemoryBuffer::Save()
	{
		if(_container == NULL) throw runtime_error("exth::MemoryBuffer::Save(): The container of the MemoryBuffer is not specific.");
		if(_container->getDBState() != FileState::FileOpened) throw runtime_error("exth::MemoryBuffer::Save(): The database files are not opened.");
		if(_pageNumber < 0) throw runtime_error("exth::MemoryBuffer::Save(): The page is not initialized. (PageNumber < 0)");
		if(_buffer == NULL) throw runtime_error("exth::MemoryBuffer::Save(): The buffer is not initialized.");

		FILE* pContentFile = NULL;
		FILE* pIndexFile = NULL;

#ifdef TEST_STATISTIC
		int ts_begin_time, ts_end_time;
#endif // TEST_STATISTIC

		switch (_dbfile)
		{
		case exth::DatabaseContentFile:
			pContentFile = _container->getContentFile();
#ifdef TEST_STATISTIC
			++ts_output_count;
			ts_begin_time = clock(); 
#endif // TEST_STATISTIC
			fseek(pContentFile, _pageNumber * _capacity, SEEK_SET);
			fwrite(_buffer, 1, _capacity, pContentFile);
#ifdef TEST_STATISTIC
			ts_end_time = clock();
			ts_output_time += ts_end_time - ts_begin_time;
#endif // TEST_STATISTIC
			break;
		case exth::DatabaseIndexFile:
			pIndexFile = _container->getIndexFile();
#ifdef TEST_STATISTIC
			++ts_output_count;
			ts_begin_time = clock(); 
#endif // TEST_STATISTIC
			fseek(pIndexFile, _pageNumber * _capacity, SEEK_SET);
			fwrite(_buffer, 1, _capacity,pIndexFile);
#ifdef TEST_STATISTIC
			ts_end_time = clock();
			ts_output_time += ts_end_time - ts_begin_time;
#endif // TEST_STATISTIC
			break;
		default:
			throw runtime_error("exth::MemoryBuffer::Save(): The database file is not specific.");
			break;
		}

		_clockScanned = false;
	}

	// @Implementation: DBController& MemoryBuffer::getContainer()
	DBController& MemoryBuffer::getContainer()
	{
		return *(_container);
	}
};

#endif // _IO_H_


