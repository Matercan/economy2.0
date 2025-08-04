#include "system/Diagnostics.h"
#include <iostream>

const std::string Color::RESET = "\032[0m ";
const std::string Color::RED = "\032[31m ";
const std::string Color::GREEN = "\032[32m ";
const std::string Color::YELLOW = "\032[33m ";
const std::string Color::BLUE = "\032[34m ";

int main() {
  Logger log;
  log.writeLine("Hello, this is a test, ");
  log.writeWarningLine("Hello, this is a warning.");
  log.writeErrorLine("Hello, this is an error.");
  log.write("This is the second line of the file.");
  cout << "Finished";

  return 0;
}
