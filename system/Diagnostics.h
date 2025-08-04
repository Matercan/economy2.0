#include <ctime>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <ostream>
#include <string>
#include <time.h>

using namespace std;
namespace fs = std::filesystem;

class Color {
public:
  static const std::string RESET;
  static const std::string RED;
  static const std::string GREEN;
  static const std::string YELLOW;
  static const std::string BLUE;
};

class TimeManager {
public:
  string currentDateFormatted() {
    // Initialize timestamp with the current time
    time_t timestamp = time(NULL);
    struct tm datetime = *localtime(&timestamp);

    char buffer[80];
    strftime(buffer, sizeof(buffer), "%Y-%m-%d", &datetime);
    return string(buffer);
  }

  string currentTimeFormatted() {
    // Initialize timestamp with the current time
    time_t timestamp = time(NULL);
    struct tm datetime = *localtime(&timestamp);

    char buffer[80];
    strftime(buffer, sizeof(buffer), "%H:%M:%S", &datetime);
    return string(buffer);
  }
};

class Logger {
public:
  // This is a constructor to initialize the log file
  Logger(bool isSubdirectory = false) {
    fs::path logDirPath;

    if (isSubdirectory) {
      logDirPath = fs::current_path() / "log";
    } else {
      logDirPath = fs::current_path().parent_path() / "log";
    }

    fs::create_directories(logDirPath);
    createLogs(logDirPath);
  }

  // A destructor to automatically close the file when the object is destroyed
  ~Logger() {
    if (logFile.is_open()) {
      logFile.close();
    }
    if (warningLogFile.is_open()) {
      warningLogFile.close();
    }
    if (errorLogFile.is_open()) {
      errorLogFile.close();
    }
  }

  // A member function to write to the log
  void write(const string &message) {
    if (logFile.is_open()) {
      logFile << timeManager.currentTimeFormatted() << " " + message;
    } else {
      cerr << "Error: log file is not open." << endl;
    }
  }

  void writeWarning(const string &message) {
    if (warningLogFile.is_open()) {
      warningLogFile << timeManager.currentTimeFormatted() << " " + message;
    } else {
      cerr << "Error: warning log file is not open." << endl;
    }
  }
  void writeError(const string &message) {
    if (errorLogFile.is_open()) {
      errorLogFile << timeManager.currentTimeFormatted() << " " + message;
    } else {
      cerr << "Error: error log file is not open." << endl;
    }
  }

  void writeLine(const string &message) { write(message + "\n"); }
  void writeWarningLine(const string &message) { writeWarning(message + "\n"); }
  void writeErrorLine(const string &message) { writeError(message + "\n"); }

private:
  ofstream logFile;
  ofstream warningLogFile;
  ofstream errorLogFile;
  TimeManager timeManager;
  Color colors;

  void createLogs(fs::path logDirPath) {
    // Name the log file with the current date
    fs::path logFilePath =
        logDirPath / (timeManager.currentDateFormatted() + ".log");

    // Open the log file for writing
    logFile.open(logFilePath, ios::app);

    logFilePath =
        logDirPath / (timeManager.currentDateFormatted() + " Warnings.log");
    warningLogFile.open(logFilePath, ios::app);

    if (!warningLogFile.is_open()) {
      cerr << "Error: failed to open log file at " << logFilePath << endl;
    }

    if (!logFile.is_open()) {
      cerr << "Error: failed to open log file at " << logFilePath << endl;
    }

    logFilePath =
        logDirPath / (timeManager.currentDateFormatted() + " Errors.log");
    errorLogFile.open(logFilePath, ios::app);

    if (!errorLogFile.is_open()) {
      cerr << "Error: failed to open log file at " << logFilePath << endl;
    }
  }
};
