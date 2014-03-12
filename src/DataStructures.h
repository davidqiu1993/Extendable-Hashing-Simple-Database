#ifndef _DATA_STRUCTURES_H_
#define _DATA_STRUCTURES_H_

#include "General.h"

namespace exth 
{
	// @Summary: The entity of Date.
	class Date
	{
	private:
		int _year;
		int _month;
		int _day;

	public:
		// @Parameters:	(void)
		// @Return:		(void)
		// @Functions:	Create an instant of Date with initialization as 2000-01-01.
		Date()
		{
			_year = 2000;
			_month = 1;
			_day = 1;
		}

		// @Parameters:	(void)
		// @Return:		(void)
		// @Functions:	Create an instant of Date with initialization of full information.
		Date(int year, int month, int day)
		{
			_year = year;
			_month = month;
			_day = day;
		}

		// @Attribute: Year
		int getYear()
		{
			return _year;
		}
		void setYear(const int value)
		{
			_year = value;
		}

		// @Attribute: Month
		int getMonth()
		{
			return _month;
		}
		void setMonth(const int value)
		{
			_month = value;
		}

		// @Attribute: Day
		int getDay()
		{
			return _day;
		}
		void setDay(const int value)
		{
			_day = value;
		}

		// @Parameters:	(void)
		// @Return:		An int indicating the int form of date
		// @Function:	Convert the date into int form.
		int toInt()
		{
			return (_year*10000 + _month*100 + _day);
		}

		// @Parameters:	value: the value to convert from
		// @Return:		(void)
		// @Function:	Convert from value in format 'YYYYMMDD' to Date.
		// @Exceptions:	invalid_argument: if the value is not a valid int form of date.
		static Date Parse(const int value)
		{
			int cyear = (int)(value / 10000);
			if(cyear < 0 || cyear > 9999) throw invalid_argument("exth::Date.Parse(int) : Invalid parsing value.");

			int cmonth = (int)((value - cyear*10000) / 100);
			if(cmonth < 1 || cmonth > 12) throw invalid_argument("exth::Date.Parse(int) : Invalid parsing value.");

			int cday = value - cyear*10000 - cmonth*100;
			switch (cmonth)
			{
			case 1:case 3:case 5:case 7:case 8:case 10:case 12:
				if(cday < 1 || cday > 31) throw invalid_argument("exth::Date.Parse(int) : Invalid parsing value.");
				break;
			case 4:case 6: case 9: case 11:
				if(cday < 1 || cday > 30) throw invalid_argument("exth::Date.Parse(int) : Invalid parsing value.");
				break;
			case 2:
				if (cday < 1 || cday > 29)
				{
					throw invalid_argument("exth::Date.Parse(int) : Invalid parsing value.");
					if (!((cyear%4==0 && cyear%100!=0) || cyear%400==0) && cday>28)
					{
						throw invalid_argument("exth::Date.Parse(int) : Invalid parsing value.");
					}
				}
				break;
			}

			return Date(cyear, cmonth, cday);
		}

		// @Parameters:	value: the value to convert from
		// @Return:		(void)
		// @Function:	Convert from value in format 'YYYY-MM-DD' to Date.
		// @Notices:	Only and all the first 10 chars will be checked, and the remaining is ignored.
		//				The format is strict, thus 0 is sometimes necessary.
		// @Exceptions:	invalid_argument: if the value is not a valid int form of date.
		static Date Parse(const char value[])
		{
			char valuex[10];
			for(int i=0;i<10;++i) valuex[i] = value[i];

			for (int i=0;i<10;++i)
			{
				valuex[i] = valuex[i] - '0';
			}

			int cyear = valuex[0]*1000 + valuex[1]*100 + valuex[2]*10 + valuex[3];
			if(cyear < 0 || cyear > 9999) throw invalid_argument("exth::Date.Parse(int) : Invalid parsing value.");

			int cmonth = valuex[5]*10 + valuex[6];
			if(cmonth < 1 || cmonth > 12) throw invalid_argument("exth::Date.Parse(int) : Invalid parsing value.");

			int cday = valuex[8]*10 + valuex[9];
			switch (cmonth)
			{
			case 1:case 3:case 5:case 7:case 8:case 10:case 12:
				if(cday < 1 || cday > 31) throw invalid_argument("exth::Date.Parse(int) : Invalid parsing value.");
				break;
			case 4:case 6: case 9: case 11:
				if(cday < 1 || cday > 30) throw invalid_argument("exth::Date.Parse(int) : Invalid parsing value.");
				break;
			case 2:
				if (cday < 1 || cday > 29)
				{
					throw invalid_argument("exth::Date.Parse(int) : Invalid parsing value.");
					if (!((cyear%4==0 && cyear%100!=0) || cyear%400==0) && cday>28)
					{
						throw invalid_argument("exth::Date.Parse(int) : Invalid parsing value.");
					}
				}
				break;
			}

			return Date(cyear, cmonth, cday);
		}
	};

	// @Summary: The entity of a record as "LINEITEM".
	class Record
	{
	private:
		int _orderKey;
		int _partKey;
		int _suppKey;
		int _lineNumber;
		double _quantity;			// Decimal(15,2)
		double _extendedPrice;		// Decimal(15,2)
		double _discount;			// Decimal(15,2)
		double _tax;				// Decimal(15,2)
		char _returnFlag;
		char _lineStatus;
		Date _shipDate;
		Date _commitDate;
		Date _receiptDate;
		char _shipInStruct[26];		// 25
		char _shipMode[11];			// 10
		char _comment[45];			// 44; var

		static const int _lenRecordBytesBaseLength = 97;	// sum of all lengths except for comment
		static const int _lenMaxRecordBytes = 142;			// sum of all lengths with comment and its last '\0'
		void* _pRecordBytes;

	public:
		// @Parameters:	(void)
		// @Return:		(void)
		// @Function:	Create an instant of Record without initialization.
		Record()
		{
			_orderKey = -1;
			_shipInStruct[0] = '\0';
			_shipMode[0] = '\0';
			_comment[0] = '\0';

			// Record bytes
			_pRecordBytes = malloc(_lenMaxRecordBytes);
		}

		// @Parameters:	origin: the origin of copy
		// @Return:		(void)
		// @Function:	Copy constructor.
		Record(const Record& origin)
		{
			this->_orderKey = origin._orderKey;
			this->_partKey = origin._partKey;
			this->_suppKey = origin._suppKey;
			this->_lineNumber = origin._lineNumber;
			this->_quantity = origin._quantity;
			this->_extendedPrice = origin._extendedPrice;
			this->_discount = origin._discount;
			this->_tax = origin._tax;
			this->_returnFlag = origin._returnFlag;
			this->_lineStatus = origin._lineStatus;
			this->_shipDate = origin._shipDate;
			this->_commitDate = origin._commitDate;
			this->_receiptDate = origin._receiptDate;
			strcpy(this->_shipInStruct, origin._shipInStruct);
			strcpy(this->_shipMode, origin._shipMode);
			strcpy(this->_comment, origin._comment);

			// Record bytes memory block
			_pRecordBytes = malloc(_lenMaxRecordBytes);
			memcpy(_pRecordBytes, origin._pRecordBytes, _lenMaxRecordBytes);
		}

		// @Parameters:	origin: the origin of copy
		// @Return:		(void)
		// @Function:	Assignment operator override.
		Record& operator =(const Record& origin)
		{
			this->_orderKey = origin._orderKey;
			this->_partKey = origin._partKey;
			this->_suppKey = origin._suppKey;
			this->_lineNumber = origin._lineNumber;
			this->_quantity = origin._quantity;
			this->_extendedPrice = origin._extendedPrice;
			this->_discount = origin._discount;
			this->_tax = origin._tax;
			this->_returnFlag = origin._returnFlag;
			this->_lineStatus = origin._lineStatus;
			this->_shipDate = origin._shipDate;
			this->_commitDate = origin._commitDate;
			this->_receiptDate = origin._receiptDate;
			strcpy(this->_shipInStruct, origin._shipInStruct);
			strcpy(this->_shipMode, origin._shipMode);
			strcpy(this->_comment, origin._comment);

			// Record bytes memory block
			_pRecordBytes = malloc(_lenMaxRecordBytes);
			memcpy(_pRecordBytes, origin._pRecordBytes, _lenMaxRecordBytes);

			return (*this);
		}

		// @Parameters:	tblRecordString: the record string in tbl file format
		// @Return:		A Record indicating the conversion result
		// @Function:	Convert a record string in tbl file format into Record class.
		static Record Parse(const string& tblRecordString)
		{
			Record record;
			string tmpstr;
			char splitter;

			istrstream iss(tblRecordString.c_str());

			// OrderKey
			iss >> record._orderKey >> splitter;

			// PartKey
			iss >> record._partKey >> splitter;

			// SuppKey
			iss >> record._suppKey >> splitter;

			// LineNumber
			iss >> record._lineNumber >> splitter;

			// Quantity
			iss >> record._quantity >> splitter;

			// ExtendedPrice
			iss >> record._extendedPrice >> splitter;

			// Discount
			iss >> record._discount >> splitter;

			// Tax
			iss >> record._tax >> splitter;

			// ReturnFlag
			iss >> record._returnFlag >> splitter;

			// LineStatus
			iss >> record._lineStatus >> splitter;

			// ShipDate
			getline(iss, tmpstr, '|');
			record._shipDate = Date::Parse(tmpstr.c_str());

			// CommitDate
			getline(iss, tmpstr, '|');
			record._commitDate = Date::Parse(tmpstr.c_str());

			// ReceiptDate
			getline(iss, tmpstr, '|');
			record._receiptDate = Date::Parse(tmpstr.c_str());

			// ShipInStruct
			getline(iss, tmpstr, '|');
			strcpy(record._shipInStruct, tmpstr.c_str());

			// ShipMode
			getline(iss, tmpstr, '|');
			strcpy(record._shipMode, tmpstr.c_str());

			// Comment
			getline(iss, tmpstr, '|');
			strcpy(record._comment, tmpstr.c_str());

			return record;
		}

		// @Parameters:	bytes: the starting address of the bytes flow of a record
		//				length: the length of the bytes flow
		// @Return:		A Record indicating the conversion result
		// @Function:	Convert a bytes flow in database file format into Record class.
		static Record Parse(void* bytes, int length)
		{
			if(length <= _lenRecordBytesBaseLength || length > _lenMaxRecordBytes) throw out_of_range("exth::Record::Parse(void*, int): The length of bytes flow is out of range = 98 to 142");

			int lenComment = length - _lenRecordBytesBaseLength;

			int i = 0;
			Record record;

			// OrderKey
			memcpy(&(record._orderKey), (char*)bytes + i, sizeof(int));
			i += sizeof(int);

			// PartKey
			memcpy(&(record._partKey), (char*)bytes + i, sizeof(int));
			i += sizeof(int);

			// SuppKey
			memcpy(&(record._suppKey), (char*)bytes + i, sizeof(int));
			i += sizeof(int);

			// LineNumber
			memcpy(&(record._lineNumber), (char*)bytes + i, sizeof(int));
			i += sizeof(int);

			// Quantity
			memcpy(&(record._quantity), (char*)bytes + i, sizeof(double));
			i += sizeof(double);

			// ExtendedPrice
			memcpy(&(record._extendedPrice), (char*)bytes + i, sizeof(double));
			i += sizeof(double);

			// Discount
			memcpy(&(record._discount), (char*)bytes + i, sizeof(double));
			i += sizeof(double);

			// Tax
			memcpy(&(record._tax), (char*)bytes + i, sizeof(double));
			i += sizeof(double);

			// ReturnFlag
			memcpy(&(record._returnFlag), (char*)bytes + i, sizeof(char));
			i += sizeof(char);

			// LineStatus
			memcpy(&(record._lineStatus), (char*)bytes + i, sizeof(char));
			i += sizeof(char);

			// ShipDate
			int shipDate_int;
			memcpy(&shipDate_int, (char*)bytes + i, sizeof(int));
			record._shipDate = Date::Parse(shipDate_int);
			i += sizeof(int);

			// CommitDate
			int commitDate_int;
			memcpy(&commitDate_int, (char*)bytes + i, sizeof(int));
			record._commitDate = Date::Parse(commitDate_int);
			i += sizeof(int);

			// ReceiptDate
			int receiptDate_int;
			memcpy(&receiptDate_int, (char*)bytes + i, sizeof(int));
			record._receiptDate = Date::Parse(receiptDate_int);
			i += sizeof(int);

			// ShipInStruct
			memcpy(record._shipInStruct, (char*)bytes + i, 25);
			record._shipInStruct[25] = '\0';
			i += 25;

			// ShipMode
			memcpy(record._shipMode, (char*)bytes + i, 10);
			record._shipMode[10] = '\0';
			i += 10;

			// Comment
			memcpy(record._comment, (char*)bytes + i, lenComment);

			// return the pointer
			return record;
		}

		// @Parameters:	resultLength: the result container for the length of the bytes flow
		// @Return:		A void* indicating the starting address of a bytes flow
		// @Function:	Convert the Record into a bytes flow in database record format, and 
		//				return the bytes flow starting address and the length of the bytes flow.
		void* const GetBytes(int& resultLength)
		{
			int lenComment = strlen(_comment);
			if (lenComment > 44) throw length_error("exth::Record::GetBytes(int&): The length of comment exceeds the maximum length = 44.");
			++lenComment; // include the last '\0'

			resultLength = _lenRecordBytesBaseLength + lenComment;
			int i = 0;

			// OrderKey
			memcpy(_pRecordBytes, &_orderKey, sizeof(int));
			i += sizeof(int);

			// PartKey
			memcpy((char*)_pRecordBytes + i, &_partKey, sizeof(int));
			i += sizeof(int);

			// SuppKey
			memcpy((char*)_pRecordBytes + i, &_suppKey, sizeof(int));
			i += sizeof(int);

			// LineNumber
			memcpy((char*)_pRecordBytes + i, &_lineNumber, sizeof(int));
			i += sizeof(int);

			// Quantity
			memcpy((char*)_pRecordBytes + i, &_quantity, sizeof(double));
			i += sizeof(double);

			// ExtendedPrice
			memcpy((char*)_pRecordBytes + i, &_extendedPrice, sizeof(double));
			i += sizeof(double);

			// Discount
			memcpy((char*)_pRecordBytes + i, &_discount, sizeof(double));
			i += sizeof(double);

			// Tax
			memcpy((char*)_pRecordBytes + i, &_tax, sizeof(double));
			i += sizeof(double);

			// ReturnFlag
			memcpy((char*)_pRecordBytes + i, &_returnFlag, sizeof(char));
			i += sizeof(char);

			// LineStatus
			memcpy((char*)_pRecordBytes + i, &_lineStatus, sizeof(char));
			i += sizeof(char);

			// ShipDate
			int shipDate_int = _shipDate.toInt();
			memcpy((char*)_pRecordBytes + i, &shipDate_int, sizeof(int));
			i += sizeof(int);

			// CommitDate
			int commitDate_int = _commitDate.toInt();
			memcpy((char*)_pRecordBytes + i, &commitDate_int, sizeof(int));
			i += sizeof(int);

			// ReceiptDate
			int receiptDate_int = _receiptDate.toInt();
			memcpy((char*)_pRecordBytes + i, &receiptDate_int, sizeof(int));
			i += sizeof(int);

			// ShipInStruct
			memcpy((char*)_pRecordBytes + i, &_shipInStruct, 25);
			i += 25;

			// ShipMode
			memcpy((char*)_pRecordBytes + i, &_shipMode, 10);
			i += 10;

			// Comment
			memcpy((char*)_pRecordBytes + i, &_comment, lenComment);

			// return the pointer
			return _pRecordBytes;
		}

		// @Parameters: (void)
		// @Return:		(void)
		// @Function:	Release all the resources.
		~Record()
		{
			free(_pRecordBytes);
		}

		// @Attribute:	OrderKey
		int getOrderKey()
		{
			return _orderKey;
		}
		void setOrderKey(const int& value)
		{
			_orderKey = value;
		}

		// @Attribute:	PartKey
		int getPartKey()
		{
			return _partKey;
		}
		void setPartKey(const int& value)
		{
			_partKey = value;
		}

		// @Attribute:	SuppKey
		int getSuppKey()
		{
			return _suppKey;
		}
		void setSuppKey(const int& value)
		{
			_suppKey = value;
		}

		// @Attribute:	LineNumber
		int getLineNumber()
		{
			return _lineNumber;
		}
		void setLineNumber(const int& value)
		{
			_lineNumber = value;
		}

		// @Attribute:	Quantity
		double getQuantity()
		{
			return _quantity;
		}
		void setQuantity(const double& value)
		{
			_quantity = value;
		}

		// @Attribute:	ExtendedPrice
		double getExtendedPrice()
		{
			return _extendedPrice;
		}
		void setExtendedPrice(const double& value)
		{
			_extendedPrice = value;
		}

		// @Attribute:	Discount
		double getDiscount()
		{
			return _discount;
		}
		void setDiscount(const double& value)
		{
			_discount = value;
		}

		// @Attribute:	Tax
		double getTax()
		{
			return _tax;
		}
		void setTax(const double& value)
		{
			_tax = value;
		}

		// @Attribute:	ReturnFlag
		char getReturnFlag()
		{
			return _returnFlag;
		}
		void setReturnFlag(const char& value)
		{
			_returnFlag = value;
		}

		// @Attribute:	LineStatus
		char getLineStatus()
		{
			return _lineStatus;
		}
		void setLineStatus(const char& value)
		{
			_lineStatus = value;
		}

		// @Attribute:	ShipDate
		Date getShipDate()
		{
			return _shipDate;
		}
		void setShipDate(const Date& value)
		{
			_shipDate = value;
		}

		// @Attribute:	CommitDate
		Date getCommitDate()
		{
			return _commitDate;
		}
		void setCommitDate(const Date& value)
		{
			_commitDate = value;
		}

		// @Attribute:	ReceiptDate
		Date getReceiptDate()
		{
			return _receiptDate;
		}
		void setReceiptDate(const Date& value)
		{
			_receiptDate = value;
		}

		// @Attribute:	ShipInStruct
		// @Notices:	0 < strlen(value) <= 26, including the last '\0'
		char* const getShipInStruct()
		{
			return _shipInStruct;
		}
		void setShipInStruct(const char* value)
		{
			if(strlen(value) > 25) throw length_error("exth::Record.setShipInStruct: the length of value exceeds the maximum length = 25.");
			strcpy(_shipInStruct, value);
		}

		// @Attribute:	ShipMode
		// @Notices:	0 < strlen(value) <= 11, including the last '\0'
		char* const getShipMode()
		{
			return _shipMode;
		}
		void setShipMode(const char* value)
		{
			if(strlen(value) > 10) throw length_error("exth::Record.setShipMode: the length of value exceeds the maximum length = 10.");
			strcpy(_shipMode, value);
		}

		// @Attribute:	Comment
		// @Notices:	0 < strlen(value) <= 45, including the last '\0'
		char* const getComment()
		{
			return _comment;
		}
		void setComment(const char* value)
		{
			if(strlen(value) > 44) throw length_error("exth::Record.setComment: the length of value exceeds the maximum length = 44.");
			strcpy(_comment, value);
		}
	};
};

#endif // _DATA_STRUCTURES_H_