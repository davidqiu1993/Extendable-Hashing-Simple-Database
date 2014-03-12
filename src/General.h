#ifndef _GENERAL_H_
#define _GENERAL_H_

#include <cstdlib>
#include <string>
#include <iostream>
#include <strstream>
#include <stdexcept>
#include <ctime>

//#define DEBUG
#ifdef DEBUG
using namespace std;
#endif // DEBUG

#define TEST_STATISTIC
#ifdef TEST_STATISTIC
using std::clock;
int ts_input_count = 0;
int ts_input_time = 0;
int ts_output_count = 0;
int ts_output_time = 0;
#endif // TEST_STATISTIC



namespace exth
{
#define INF 10000000

	using std::string;				// C++ Data Structure:	string
	using std::istrstream;			// C++ Data Structure:	istrstream
	using std::getline;				// C++ Function:		getline
	using std::system;				// System Call:			system
	using std::invalid_argument;	// Standard Exception:	invalid_argument
	using std::length_error;		// Standard Exception:	length_error
	using std::out_of_range;		// Standard Exception:	out_of_range
	using std::domain_error;		// Standard Exception:	domain_error
	using std::runtime_error;		// Standard Exception:	runtime_error
	using std::underflow_error;		// Standard Exception:	underflow_error
	using std::overflow_error;		// Standard Exception:	overflow_error
};

#include "DataStructures.h"
#include "IO.h"
#include "HashIndex.h"
#include "Bucket.h"
#include "TestData.h"
#include "HighHashManagement.h"
#include "LowHashManagement.h"

#endif // _GENERAL_H_