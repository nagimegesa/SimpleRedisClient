//
// Created by computer on 2026/9/19.
//


#include <iostream>
#include "Nacos.h"

using namespace std;
using namespace nacos;

int main() {
    Properties props;
    props[PropertyKeyConst::SERVER_ADDR] = "127.0.0.1:8848";//Server address
    props[PropertyKeyConst::AUTH_PASSWORD] = "nacos";
    props[PropertyKeyConst::AUTH_USERNAME] = "nacos";
    auto *factory = nacos::NacosFactoryFactory::getNacosFactory(props);
    NamingService* nameService = factory->CreateNamingService();
    nameService->registerInstance("hello", "127.0.0.1", 8080);

    std::string s;
    std::cin >> s;
    return 0;
}