// Independent SHA-256 expected values retained from the pre-integration 864-case
// Python hashlib/known-vector controls. No expected hash is computed by either
// implementation under test. Binary input byte i is (i * 131 + length) % 256.
// Native discovery can run this test directly; it needs no Python or input file.
#include "commands/BackupManifestCreationSha256.h"
#include "common/sha256.h"
#include <array>
#include <cstddef>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {
enum class InputKind { Empty, Abc, MillionA, Binary };
struct Vector {
    const char* name;
    std::size_t bytes;
    InputKind kind;
    const char* expected;
};
constexpr Vector vectors[] = {
    {"empty", 0, InputKind::Empty, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
    {"abc", 3, InputKind::Abc, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
    {"million-a", 1000000, InputKind::MillionA, "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"},
    {"binary-0", 0, InputKind::Binary, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
    {"binary-1", 1, InputKind::Binary, "4bf5122f344554c53bde2ebb8cd2b7e3d1600ad631c385a5d7cce23c7785459a"},
    {"binary-2", 2, InputKind::Binary, "d52956a8a8fda564362226fe968a3a8cea4a6a79842a70dbab8a06dae211c5ed"},
    {"binary-3", 3, InputKind::Binary, "3d29e3f6c20f56e15d8c76455cc7e9bd5fb9107163bb9ebb5a4dcc969629f46b"},
    {"binary-4", 4, InputKind::Binary, "a20eb2c54333b361630a924dcc0cb2d3ffbe2638727c3aa0f5a22e38dd3652ee"},
    {"binary-5", 5, InputKind::Binary, "be863439edc4a43a058628975726a2fb3cbb10c81fe87ca3d7dfe12e42f90e4d"},
    {"binary-6", 6, InputKind::Binary, "9965e95f75d112e599ce19b4e5039ee62c04a729cbafaa6be207ba3195c7f472"},
    {"binary-7", 7, InputKind::Binary, "d68796b7712ef368707a713257728d066d10964d9ac113a284888979415d588a"},
    {"binary-8", 8, InputKind::Binary, "7742b5e04d37a7ce46a5589480c19170d8c5db50b8254b751889c4b1b63fd493"},
    {"binary-9", 9, InputKind::Binary, "fb929ff06f60a4f4710d67a81c1ec1de239230a85581d0b4d80bddcfe080ebb0"},
    {"binary-10", 10, InputKind::Binary, "02d7809f794588296e1577cce9fab48c88c9d80551f308953e31e1f9292d7d05"},
    {"binary-11", 11, InputKind::Binary, "e0be73b5b0922426fdb1eae0b7ed0fb4337c24adc29642b5fa0abbed688833f5"},
    {"binary-12", 12, InputKind::Binary, "b8a1eb6310740bd6c7be891d08d9d1a7860c2c7b6e7e1a3e02e4b33a69b95275"},
    {"binary-13", 13, InputKind::Binary, "340fec5bc5bed451992a791d21ae6f69d7f73b21bda706618c032ed789f623c5"},
    {"binary-14", 14, InputKind::Binary, "c72f82cb09f6c6f407bdbc756dd7111377a342ea838b2827c3fe9b7aa855d9aa"},
    {"binary-15", 15, InputKind::Binary, "fd0bc3de02de1de7e63b10bb62b35ed62982e98d103d653ae505ccb366feab97"},
    {"binary-16", 16, InputKind::Binary, "7ac3e87a2a4a5832a946f56cdec777cf30003d67f1e06eba7660a8b9ce2b7d90"},
    {"binary-17", 17, InputKind::Binary, "6439f3d1c69dd4bc86d09bce2f09aedd6a3e83aaf7ccd762c4d37faa89f2812b"},
    {"binary-18", 18, InputKind::Binary, "dd5715b2b243640ba80cd250c673a6a8be84d3dfd420e1fdf9ba78bcc4cb810d"},
    {"binary-19", 19, InputKind::Binary, "969da5fb258ebfedd0f8dfbf83995584f35dce5f49c17c20b4861af112276146"},
    {"binary-20", 20, InputKind::Binary, "c411e4adbab0a51accaf90c15e48aa4e57ec466adb6af994d6d87b5561a8f0d5"},
    {"binary-21", 21, InputKind::Binary, "dc5587d2b83c6d243a5624c9645f192de6e5a075f18dc01c5860244ef5f82a65"},
    {"binary-22", 22, InputKind::Binary, "4efbc2d334fd84ce7d285defd95f7c5dbfa1d95de7a26c033fcd63fda81a8f0f"},
    {"binary-23", 23, InputKind::Binary, "44a02c64e60a394c81e44b54a11aca7ec3f84998291a90d4a2dd883b5e849539"},
    {"binary-24", 24, InputKind::Binary, "705a5f73a821a7a97e8996da1fd31c85ca9732aab0d03e3ffa4992ecb2ba2b40"},
    {"binary-25", 25, InputKind::Binary, "7640bb1accccb4f3fb4204f4e5ec33512d1a1dc572d5a3f8f2a486e18a03ba01"},
    {"binary-26", 26, InputKind::Binary, "9453277e0d542623b839d980b373c2c774da3605a0939da7ad4106be5b4982dd"},
    {"binary-27", 27, InputKind::Binary, "366b6dc4acdbef2b41c7ec9dbf1d641349059b5cf7e994b97c05adbc5620ff18"},
    {"binary-28", 28, InputKind::Binary, "52ff721a3be076d79e53d4ca466d71d07841fbf57ac5a630eef71fc87fd2bc3c"},
    {"binary-29", 29, InputKind::Binary, "1b0962734bd64463a8fd62026d4f89d71b1e3a960fce21cd869c9564638fb44b"},
    {"binary-30", 30, InputKind::Binary, "d8f4be1e8e108d29d454f581eeebf77919c7286ceab37d00ae1460eb485a94e9"},
    {"binary-31", 31, InputKind::Binary, "1439b4e6c3253c52732631b745015cf6fe65e4101ca72b5d13ef17bcfdd9cae6"},
    {"binary-32", 32, InputKind::Binary, "ccecf31f3de5927340d13afdf284aa96b34aca8b3a876d7a59c3dc25830fcb2e"},
    {"binary-33", 33, InputKind::Binary, "2a4ca01e6ee3d8fc864007131e8c431a625b8fc6d5fcbb527ae2b21834b025ee"},
    {"binary-34", 34, InputKind::Binary, "945945115db62ad00f87d90fb238315aa5a1bebc7824f6f3ddee3530c9818b64"},
    {"binary-35", 35, InputKind::Binary, "6b20772ce4a9c382821f0144082c12ebb2c64a8f0adbeb27510d346f95e6afba"},
    {"binary-36", 36, InputKind::Binary, "17b6958610455e1cb803969754d54f4b149ea2a1e0cf50cd0becc2f3b98420a1"},
    {"binary-37", 37, InputKind::Binary, "e29961fbeaf33ce2c4a2adf840538fbbd353295514da479d16f92c76e510ec7e"},
    {"binary-38", 38, InputKind::Binary, "ce206803e4e5fa09162dcd845bdea25d9a9123827ec9d09716954a4b3450fe27"},
    {"binary-39", 39, InputKind::Binary, "f05b60ad85d6d87706114c06573466bf2820139d464788e76c7877e30984a835"},
    {"binary-40", 40, InputKind::Binary, "a7a0124f9e39b3f0e7e4c81e4abdc0b83b9729f1ec6771db549b80d10ffb36d2"},
    {"binary-41", 41, InputKind::Binary, "daf31b5b2312ae488b3561ec2fa758219c4fa4b5eab9e03032bf011451ad0139"},
    {"binary-42", 42, InputKind::Binary, "cbb6487d99b8a7b676603e4f67669fed65072468f21de93422bf5df8205799e4"},
    {"binary-43", 43, InputKind::Binary, "15d2fd7e1feae64a400efb16d20ff88f51eaa18ff64c326db1841f316686894f"},
    {"binary-44", 44, InputKind::Binary, "886f9aab4a2ab628d5dd5f185a900f4bd96183f826793443189a10f4125a8308"},
    {"binary-45", 45, InputKind::Binary, "11bcec2760cabfcc7ea709464300f1d3fd652452c0d6ff669bd551e30f64c0d2"},
    {"binary-46", 46, InputKind::Binary, "1c24afa719e19e88821e54693b0f408c724a3e16b1fe6e1d319fdaa76d5d0bf3"},
    {"binary-47", 47, InputKind::Binary, "f062f0d095105e8dabdc0755a9d939019326af8ce96a857c4df4511b9062a1f8"},
    {"binary-48", 48, InputKind::Binary, "ca9873bca930df1733b90a0feffc1c45e5990c791aceb3b38c2c95bb995b0d0a"},
    {"binary-49", 49, InputKind::Binary, "5e6e316b6b6cf1708e702da9b0111f13700f91d883106cac9641c378c002973c"},
    {"binary-50", 50, InputKind::Binary, "11d290eb8c6647edcbe8bd8a2a28eeda58deeadfcec096fc33dcb0918bf639a6"},
    {"binary-51", 51, InputKind::Binary, "d63d3cff7a80f2035345b9da1777d6ee7a981bd7c202a338e7377b3ad565f135"},
    {"binary-52", 52, InputKind::Binary, "b16f73a94f4c39feb4c0ffea181c35738bca4f736a862f1936ab4b2d5d0e37c6"},
    {"binary-53", 53, InputKind::Binary, "2c41d01341220dab39f40afba184b575a0f41f31bad4d75b45b3c458e0288f4f"},
    {"binary-54", 54, InputKind::Binary, "380710b58d249f807c222a725f0a2eae6cb29c762c72f8308abe2c4cfe3c596c"},
    {"binary-55", 55, InputKind::Binary, "86bdf1401483a3d48aa0b853de242db9073b3e9e190bd1c8073abf0eba3d3ea7"},
    {"binary-56", 56, InputKind::Binary, "3682950d356858453c48a040cf03790b27a8a5bb541df1c7b2741140f1ca041d"},
    {"binary-57", 57, InputKind::Binary, "a1a4f023df6eac2b87153a848c362a381988a3034d6078abb92b1c5618f08def"},
    {"binary-58", 58, InputKind::Binary, "8eb76c1a4491a860d222f3702b3470d5e2f8a2e770b98490c56d59aa70789300"},
    {"binary-59", 59, InputKind::Binary, "d8388db8c8f1f821ae6af7f95e64ed9937b237618c465380febdf108c9120785"},
    {"binary-60", 60, InputKind::Binary, "87c1b71eb2145ac18840a03c6bbc61546f62ed5a9447c83e3d1cab2dd8740413"},
    {"binary-61", 61, InputKind::Binary, "27b4ff445a7a7e6ffe6ff2d670126c1de321a70de6a31008064f68656cd0400f"},
    {"binary-62", 62, InputKind::Binary, "05bac2c5deed33f6aeff5814267a36b7c5e70e25e5361cf6b6d5ac4ebd883e14"},
    {"binary-63", 63, InputKind::Binary, "308f063ec4dcb7154b236d356430791a13a7b3001d6a375dcfd686368c68950f"},
    {"binary-64", 64, InputKind::Binary, "197a5d2e6d3bc917955f5c5f4fd66bf316cb07ee58f6110a730c08c11b5563e0"},
    {"binary-65", 65, InputKind::Binary, "0e0450689e33dcf6ce90264391ad6f82e0efe546579e57c0fe8dc759a37fe26a"},
    {"binary-66", 66, InputKind::Binary, "42fb2e72222d19de141404f5cef615b6d80ed778091691e8a087fbcef41d9819"},
    {"binary-67", 67, InputKind::Binary, "cf7e61f6d1fb7ca2889f9fa60979f7895a56d0c74dc5642591560c41500d2e74"},
    {"binary-68", 68, InputKind::Binary, "ad42570a46780ea2895e1d022df6a14f24e94b276d32ae573acaa6ac170d68f9"},
    {"binary-69", 69, InputKind::Binary, "4a5b624eaea8a4a538476e7f17434f7ecd671d4d7008549e788099ce5dbef279"},
    {"binary-70", 70, InputKind::Binary, "9cf8d057a04bee951a169e685d789aaf7bc5c786c33effddb537181f824857da"},
    {"binary-71", 71, InputKind::Binary, "c15317bf145d5ebd21a2ada030291a447c70455f27f59e445f43879f17fdf5fc"},
    {"binary-72", 72, InputKind::Binary, "1a8fba7e0d061add1f2d38625532ddc6bdd5c2143ed0f1ce8fd94e9bb9942cbe"},
    {"binary-73", 73, InputKind::Binary, "2c4c3ecc1c4c98ac35f1d405a50cbe560ecf5e17e6c3f4e089f97a4d5df836cd"},
    {"binary-74", 74, InputKind::Binary, "f8485a9c7786770af84de78d14bebe100fb1327382de9939f0fbbad4b43e34c0"},
    {"binary-75", 75, InputKind::Binary, "196cd18d763be3b6f72f726f10dc9166f26abe06ea7898222f710194695f914f"},
    {"binary-76", 76, InputKind::Binary, "25d90c8cd89508549b7763c0cd104c1ce3eb5caf05286c023a5fa04282c1d7dd"},
    {"binary-77", 77, InputKind::Binary, "fe9941587685f9578029d5662aba8fec6cad96dacc3a73529938a86fc9b9121f"},
    {"binary-78", 78, InputKind::Binary, "712bf09a622f3260b55c64a02e91e1e709e92b0fd6aac68e8f33a3bf19188b17"},
    {"binary-79", 79, InputKind::Binary, "aa6e0c6cdcb46dac5eff52443f0d6db66f14a143536c945ec0209536916f4de4"},
    {"binary-80", 80, InputKind::Binary, "a2af7c8e473669307d4ec0a17a6d930396da5eda4e4350a3e4ca5d81d6270902"},
    {"binary-81", 81, InputKind::Binary, "20a4c75b3fd19f082c8868190c0b052bd836bad221b5542a81c048f1165462f7"},
    {"binary-82", 82, InputKind::Binary, "c4bd4a77846494d6e8b59c0b4ccad622e98ba9fd7d8d0f54245294535560664c"},
    {"binary-83", 83, InputKind::Binary, "3c9ca8346fa474e6ca8b62eb81beb38ba3a399781fe972074ec0b06dec2583b3"},
    {"binary-84", 84, InputKind::Binary, "d073b344c736318d3c35f97f697555914657aa43e81534d3e6064b14671d9fff"},
    {"binary-85", 85, InputKind::Binary, "37e4ca4899106b7799c11ed287e0d0bce8f92b6bad1abc9ba9bb9f4381c6cab7"},
    {"binary-86", 86, InputKind::Binary, "51df6ed794b4364a700d4e8dbcc9e48d6bb0a75e08d34a42f962390ba4ba3095"},
    {"binary-87", 87, InputKind::Binary, "27ce91e712696131645adde5f6db6e229cdd9a856c94fc4a68fca624d567bb3e"},
    {"binary-88", 88, InputKind::Binary, "fbde65a5516f761efb74f542acd29fa967f81de5b950d30b0266eb85889d6097"},
    {"binary-89", 89, InputKind::Binary, "288d0cf406a456c975f3229ba42f564a31c703880b6f0807cd86afe54e97ff72"},
    {"binary-90", 90, InputKind::Binary, "18e1df7d78d816e70b2343fec8b8200263338b5a2acac13046ff7b10690007a3"},
    {"binary-91", 91, InputKind::Binary, "f800f66291377d9f1719fcc3ef4eec6f884ebe4cea46b297e063efb80f98b29e"},
    {"binary-92", 92, InputKind::Binary, "1f16ff7d492cc3af0045bda520c82fdb6ecac2ff5e5523e245e3e23352b33da2"},
    {"binary-93", 93, InputKind::Binary, "472c13f1f05ada1653592d36979161ed9d314985e234839ef813915b6c96194f"},
    {"binary-94", 94, InputKind::Binary, "a7b02bd86687e2f800d896d1a875be645835e2d73b5652850cfaa93bbe5418cd"},
    {"binary-95", 95, InputKind::Binary, "281fa80b00ecaee489fccdb675a56cbd35964d7bde0db8d924e7a9cc6c8c1799"},
    {"binary-96", 96, InputKind::Binary, "0a6966f5de35043f992bd3a5aa2fd2008abb82567b40455f6d3fafd0ab14a822"},
    {"binary-97", 97, InputKind::Binary, "8db7b720379c6cc8831c39bddd8ea6344fcbe3b85605ac0c170bff862aa05d49"},
    {"binary-98", 98, InputKind::Binary, "505ebe0446ecc0d0c4132af99ba95e4e328ba8bab3ea363bccc815f2af379cf2"},
    {"binary-99", 99, InputKind::Binary, "2b7e9d5554a3e3233ff9cff3d391e50b64a96d010cc2ab712045910e5928a792"},
    {"binary-100", 100, InputKind::Binary, "8d1b547510d9dcf18f58cbb65ad8deffdfd2cc6f8cce2e3c88aee7910543b4fc"},
    {"binary-101", 101, InputKind::Binary, "2e275ec7888e928e133d804d0a9476e66d5c95e06ef836b2d5dc5f0d2ddfa287"},
    {"binary-102", 102, InputKind::Binary, "e4d36ddbd39030993d3cdecbc888d1165e3fbcd8b8cfaabd5733db5ba16a6102"},
    {"binary-103", 103, InputKind::Binary, "156b9b980c8ef6e6ceb4dd17d04d9cd70a5780088fa9d8a8829b3a9da288f6be"},
    {"binary-104", 104, InputKind::Binary, "8a4ae11f41fdc86c2b39da5fff64ca3ca1034c92acc7a608a2ca26ca8274f81f"},
    {"binary-105", 105, InputKind::Binary, "fa42dfaedf15f67473229708f61e36a8bf88ae0c371cf17414541ec7063cc4a6"},
    {"binary-106", 106, InputKind::Binary, "3a60599978240e525f990bc336350d13397f19c0ee5b5bf70f030cba658ae70b"},
    {"binary-107", 107, InputKind::Binary, "d2b6eec83fe6b930e2f6731e458dfacb9aeeb3a565b9a47be5cab7b2cfeda003"},
    {"binary-108", 108, InputKind::Binary, "dc69c96b27875faf1cbdc3ee677be5120de815f55657ef4855ffc9fb11f7fda1"},
    {"binary-109", 109, InputKind::Binary, "5653e4acba0e2b19b44179950c12907a8044d91a9d8dd72803b464eae1b7b888"},
    {"binary-110", 110, InputKind::Binary, "84b3d2ca3ed26de257975a3013fe1b4e4a466727ac8e59c98b591a366fc2fbe8"},
    {"binary-111", 111, InputKind::Binary, "efba2da7debf9e1b7180afc08ec4a93274e7ef39a98784adb181b8e70f48f660"},
    {"binary-112", 112, InputKind::Binary, "7630abbb1df261e1c03a6d3c81984305b37d4a26a5add2d207170793cd3d5211"},
    {"binary-113", 113, InputKind::Binary, "073560999d10bf2c77ad8ec9884f716cad2d24ea0325e5b9476dd70e78909aae"},
    {"binary-114", 114, InputKind::Binary, "8051ed54832fabc590eb95879439a63c3e7334edbaed56101970b0e9a6d13216"},
    {"binary-115", 115, InputKind::Binary, "60f99bb9a0d9b5fcb125dafafc6b973f4f5b1e0eb0f2bab661189ffc38b6c79b"},
    {"binary-116", 116, InputKind::Binary, "a07cb5ab880ce0894c013a2fc4d31ead40f5be91072c4366b184d68eb7c0ea02"},
    {"binary-117", 117, InputKind::Binary, "07f6d3f91ee04fd00e87b845c7a9718ac5c8ba466191ee5aac93e6284819d706"},
    {"binary-118", 118, InputKind::Binary, "87449ee8b2ae46d859e460dbe61e7817b1ec98e85a4ccbd0cbcecfed6e863279"},
    {"binary-119", 119, InputKind::Binary, "de55d54be9f6c0db65425d6e308db6ba9e8eea6e35b070e2dcd39919b668ff9c"},
    {"binary-120", 120, InputKind::Binary, "033f2213655dffe41d5824f3babf21b7f7dcb731d1c6ff1835b4937b2a16db00"},
    {"binary-121", 121, InputKind::Binary, "176d6fb95fe0f59b13e55e7aabe4e8b3afe1a7a4d980a07cb6b5615ef26eee9a"},
    {"binary-122", 122, InputKind::Binary, "e5eefc2960e98194905dbb849e73401f6a9a51b7974df01017dedc2a562dafad"},
    {"binary-123", 123, InputKind::Binary, "5eb0b892feb0e11a885c64c9d5d6ded7d8bc7e90511abcb8d5870d104fcf9c07"},
    {"binary-124", 124, InputKind::Binary, "4399049063e67d597d58af597190712b89a9e13b2d8d809326a0773657fdfa01"},
    {"binary-125", 125, InputKind::Binary, "6050ee8aaaa185499253836c0db8524d4711af08f408007191e5e3f619102c6d"},
    {"binary-126", 126, InputKind::Binary, "4ab41fed0b275f5783df6e66ea67608ebc0d11930a3133dae1d04494b9045813"},
    {"binary-127", 127, InputKind::Binary, "e11b02085cb403ba057be515f657dd7ccd525faa3f46eea4f3c1b80e1e0afc07"},
    {"binary-128", 128, InputKind::Binary, "22a665a43655bc61baeb62241a1bb247b0d8ce7c6352dbcbb035e73b99c310b2"},
    {"binary-129", 129, InputKind::Binary, "2b663e72b7901994a87ff501473d3e796cca3fe69e39bdcb0419ef4c70d69003"},
    {"binary-130", 130, InputKind::Binary, "c77e920cebd2e228ae3f7a9176eaefcdeb58d3f77d518a3f30cce8644a0d8c28"},
    {"binary-255", 255, InputKind::Binary, "b09a2141d879ce93a3d2045de7f2f28af8619986ab7895be5d89814634d9ddb7"},
    {"binary-256", 256, InputKind::Binary, "3312ebee214f09971b3a69a0a732248325d7ac0dc755db0bbcbc5cb663365fa0"},
    {"binary-257", 257, InputKind::Binary, "ee399caa9c1f8a4dbee40adda5189e990679ec381f5384fc57704dc8ecdcd9eb"},
    {"binary-4095", 4095, InputKind::Binary, "fd060479f4f387d78aa0fe1fde9ecc05853818b9fa188832c7fb0702aceb2f66"},
    {"binary-4096", 4096, InputKind::Binary, "66ad3905f04032782f735166f0527e4280b1d872765a55b5df2b2a73ac3248e8"},
    {"binary-4097", 4097, InputKind::Binary, "f31ad9f54d37cd14100d210ee985d5b5982a1eee821a0a30d38dbb7c7048af75"},
    {"binary-65535", 65535, InputKind::Binary, "e647b4f18d3a6774e9e57acbf297fa263fed80825ec0186ce41de2db9b139fd8"},
    {"binary-65536", 65536, InputKind::Binary, "0c23493796ed952152d9e0c5f1afcae3940d22f0882411a2b88698a9aef2824b"},
    {"binary-65537", 65537, InputKind::Binary, "a3ba648e9de34f9b036dbc656eb02f16610c4dbecd62e7021fdeaa77c74a3879"},
    {"binary-1048576", 1048576, InputKind::Binary, "cd7b4e03e10ac91303f3471c7f1806ac1af91c879364664e1499f9582f59adf5"},
};

// This append/chunk helper is byteexact to the tested external component driver.
template<class Hash>static std::string calculate(const char*data,size_t size,const std::vector<size_t>&chunks){Hash digest;size_t pos=0,index=0;bool progress=false;for(auto n:chunks)progress|=n!=0;if(size&&(!progress||chunks.empty()))throw std::runtime_error("nonprogresschunkplan");digest.append(nullptr,0);while(pos<size){size_t n=chunks[index++%chunks.size()];if(!n){digest.append(nullptr,0);continue;}if(n>size-pos)n=size-pos;digest.append(data+pos,n);pos+=n;}return digest.finishHex();}

std::string inputBytes(const Vector& vector) {
    if (vector.kind == InputKind::Empty) return {};
    if (vector.kind == InputKind::Abc) return "abc";
    if (vector.kind == InputKind::MillionA) return std::string(1000000, 'a');
    std::string bytes(vector.bytes, '\0');
    for (std::size_t i = 0; i < bytes.size(); ++i)
        bytes[i] = static_cast<char>((i * 131 + vector.bytes) % 256);
    return bytes;
}
}  // namespace

int main() {
    std::size_t controls = 0;
    try {
        const std::array<std::vector<std::size_t>, 6> patterns = {{
            {std::size_t(1) << 30}, {1}, {2, 3, 5, 7},
            {63, 64, 65}, {0, 1, 0, 64, 1024}, {65536}
        }};
        for (const auto& vector : vectors) {
            const std::string bytes = inputBytes(vector);
            if (bytes.size() != vector.bytes)
                throw std::runtime_error(std::string(vector.name) + " input size");
            for (const auto& chunks : patterns) {
                const auto legacy = calculate<SHA256>(bytes.data(), bytes.size(), chunks);
                const auto creation = calculate<dbms::backup_manifest_detail::ManifestCreationSha256>(
                    bytes.data(), bytes.size(), chunks);
                const std::string unaligned = std::string("!") + bytes;
                const auto shifted = calculate<dbms::backup_manifest_detail::ManifestCreationSha256>(
                    unaligned.data() + 1, bytes.size(), chunks);
                if (legacy != vector.expected || creation != vector.expected ||
                    shifted != vector.expected)
                    throw std::runtime_error(std::string(vector.name) + " independent SHA mismatch");
                ++controls;
            }
        }
        if (controls != 864) throw std::runtime_error("control count");
        std::cout << "Tests run: " << controls << "\nTests failed: 0\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "DIGEST_COMPONENT_FAILURE completed=" << controls << ' '
                  << error.what() << '\n';
        std::cout << "Tests run: " << controls + 1 << "\nTests failed: 1\n";
        return 1;
    }
}
