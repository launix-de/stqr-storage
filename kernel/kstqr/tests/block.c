/* SPDX-License-Identifier: GPL-3.0-only */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <linux/fs.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <unistd.h>
#define CHECK(x)                                                                                   \
	do {                                                                                       \
		if (!(x)) {                                                                        \
			fprintf(stderr, "block line %d: %s (%s)\n", __LINE__, #x,                  \
				strerror(errno));                                                  \
			exit(1);                                                                   \
		}                                                                                  \
	} while (0)
#define M (1U << 20)
int main(int argc, char **argv)
{
	void *memory;
	unsigned char *p;
	int fd, sector;
	uint64_t size, trim[2];
	struct stat st;
	CHECK(argc == 3);
	CHECK(!stat(argv[1], &st) && S_ISBLK(st.st_mode));
	CHECK(!posix_memalign(&memory, 4096, 2 * M));
	p = memory;
	fd = open(argv[1], O_RDONLY | O_DIRECT);
	CHECK(fd >= 0);
	CHECK(!ioctl(fd, BLKGETSIZE64, &size) && size == 4 * M);
	CHECK(!ioctl(fd, BLKSSZGET, &sector) && sector == 512);
	CHECK(!close(fd));
	if (!strcmp(argv[2], "ro")) {
		CHECK(open(argv[1], O_RDWR) < 0 && (errno == EROFS || errno == EACCES));
		free(memory);
		return 0;
	}
	fd = open(argv[1], O_RDWR | O_DIRECT);
	CHECK(fd >= 0);
	memset(p, 0xff, 4096);
	CHECK(pread(fd, p, 4096, 3 * M) == 4096);
	for (int j = 0; j < 4096; j++)
		CHECK(p[j] == 0);
	CHECK(pread(fd, p, 4096, 0) == 4096 && !memcmp(p, "from Go", 7));
	for (int j = 7; j < 4096; j++)
		CHECK(p[j] == 0); /* short stored stripe tail */
	memset(p, 'S', 512);
	CHECK(pwrite(fd, p, 512, 4096) == 512);
	CHECK(!fsync(fd));
	memset(p, 'X', 1024);
	CHECK(pwrite(fd, p, 1024, M - 512) == 1024);
	CHECK(!fsync(fd));
	memset(p, 0, 1024);
	CHECK(pread(fd, p, 1024, M - 512) == 1024);
	for (int j = 0; j < 1024; j++)
		CHECK(p[j] == 'X');
	memset(p, 'D', M);
	CHECK(pwrite(fd, p, M, 2 * M) == M);
	CHECK(!fsync(fd));
	trim[0] = 2 * M;
	trim[1] = M;
	CHECK(!ioctl(fd, BLKDISCARD, trim));
	CHECK(!fsync(fd));
	memset(p, 0xff, M);
	CHECK(pread(fd, p, M, 2 * M) == M);
	for (unsigned j = 0; j < M; j++)
		CHECK(!p[j]);
	trim[0] = M;
	trim[1] = 512;
	CHECK(!ioctl(fd, BLKDISCARD, trim));
	CHECK(!fsync(fd));
	CHECK(pread(fd, p, 1024, M - 512) == 1024);
	for (int j = 0; j < 512; j++)
		CHECK(p[j] == 'X' && p[j + 512] == 0);
	CHECK(!close(fd));
	free(memory);
	puts("KSTQR_BLOCK_IO_PASS");
	return 0;
}
