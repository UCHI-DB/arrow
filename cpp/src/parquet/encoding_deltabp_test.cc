//
// Created by harper on 3/28/20.
//

#include <gtest/gtest.h>
#include <fstream>
#include <sys/mman.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <vector>
#include <memory>
#include "encoding.h"

namespace parquet {

    namespace test {

        using namespace std;

        TEST(DeltaBpDecoding, DecodeFile) {
            std::ifstream infile("testres/encoding/deltabp.txt");
            int lineval;
            vector<int32_t> buffer;
            while (infile >> lineval) {
                buffer.push_back(lineval);
            }

            struct stat st;
            stat("testres/encoding/deltabp", &st);
            auto file_size = st.st_size;
            auto fd = open("testres/encoding/deltabp", O_RDONLY, 0);
            //Execute mmap
            void *mmappedData = mmap(NULL, file_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
            assert(mmappedData != MAP_FAILED);
            auto content = (uint8_t *) mmappedData;

            auto decoder_raw = MakeDecoder(Type::INT32, Encoding::DELTA_BINARY_PACKED, nullptr);

            auto decoder = dynamic_cast<TypedDecoder<Int32Type>*>(decoder_raw.get());
            decoder->SetData(buffer.size(), content, file_size);

            vector<int32_t> buffer2(buffer.size());
            decoder->Decode(buffer2.data(), buffer.size());

            for(int i = 0 ; i < buffer.size(); ++i) {
                EXPECT_EQ(buffer[i], buffer2[i]);
            }


            munmap(content, file_size);
            close(fd);
        }


    }
}