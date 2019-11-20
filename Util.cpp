#include "Util.h"

void SplitString(const std::string& str, char ch, std::vector<std::string>& dest) {
    size_t pre_pos = 0;
    for (size_t i = 0;i < str.length(); ++i ) {
        if ( str[i] == ch ) {
            dest.push_back( str.substr(pre_pos, i - pre_pos) );
            pre_pos = i + 1;
        }
    }
}
