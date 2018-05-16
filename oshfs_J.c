/*
��һ�׶δ���1. mknod�е�name����һ��ʼûʹ�����飬ʹ��ָ��û�з���ռ䵼�¶δ���
				2.mknod�в�ѯimap��ʱ������imap==0���ж�д����imap=0�����´���
			��������������ɺ��ļ���дɾ������������
*/

#define FUSE_USE_VERSION 26
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <fuse.h>
#include <sys/mman.h>
#include <inttypes.h>
#define FCSIZE 32*1024-8-4-256-256-sizeof(struct stat)  //��Ϣblock��������
#define NCSIZE 32*1024-8-4         //��ͨblock��������
#define FILE 1
#define DIR 0

struct Inode{
    int32_t firstblockID;
    int32_t info;                //�ļ�orĿ¼
    void *first_content;
};

struct NormalBlock{
    struct NormalBlock *next;
    int32_t num;
    char content[NCSIZE];
};

struct FirstBlock{
    struct NormalBlock *next;
    int32_t num;
    char path[256];
    char name[256];
    struct stat st;
    char content[FCSIZE];
};

static const size_t size = 1 * 1024 * 1024 * (size_t)1024;
static void *block[32*1024];
static struct FirstBlock *fblock[32*1024];                   //��ʽ����FirstBlock
static struct NormalBlock *nblock[32*1024];                  //��ʽ����nblock
static struct Inode *inode[32*1024];
static char *imap, *bmap; //���ֽڵ�imap��bmap��

static int get_inode(const char *path)
{
    printf("get_inode: %s\n", path);
    size_t blocknr = sizeof(block) / sizeof(block[0]);
    size_t blocksize = size / blocknr;
    size_t maxinode = blocknr;
    //��ѯimap��
    for(int i=0; i<maxinode; i++)
    {
        if(imap[i]==1)
        {
            printf("%d  |  \n", inode[i]->firstblockID);
            printf("{%s:%s}", fblock[inode[i]->firstblockID]->path, path);
            if(strcmp(fblock[inode[i]->firstblockID]->path, path)==0)
            {
                printf("get_inode end1\n");
                return i;
            }
        }
    }
    printf("get_inode end2\n");
    return -1;
}


static int new_block()
{
    printf("new_block\n");
    size_t blocknr = sizeof(block) / sizeof(block[0]);
    size_t blocksize = size / blocknr;
    //��ѯbmap��
    for(int i=0; i<blocknr; i++)
    {
        if(bmap[i]==0)
        {
            bmap[i] = 1;
            //�����
            block[i] = mmap(NULL, blocksize, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
            //��ʼ��
            memset(block[i], 0, blocksize);
            //���ָ�ʽ��������¼���
            fblock[i] = (struct FirstBlock *)block[i];
            nblock[i] = (struct NormalBlock *)block[i];
            nblock[i]->num = i;
            printf("new_block: %d", i);
            printf("new_block end\n");
            return i; //���ؿ�ID
        }
    }
    printf("NO enough space!");
    return -1;
}

static int release_block(void *node)
{
    printf("release_block\n");
    size_t blocknr = sizeof(block) / sizeof(block[0]);
    size_t blocksize = size / blocknr;
    struct NormalBlock *nnodep, *nnodeq;
    nnodep = (struct NormalBlock *)node;
    nnodeq = nnodep;
    while(nnodep->next!=NULL)
    {
        nnodep = nnodep->next;
        bmap[nnodeq->num] = 0;
        printf("release %d block", nnodeq->num);
        munmap(nnodeq, blocksize);
        nnodeq = nnodep;
    }
    bmap[nnodeq->num] = 0;
    printf("release %d block", nnodeq->num);
    munmap(nnodeq, blocksize);
    printf("release_block end\n");
    return 0;
}

static void output()
{
    for(int i=0; i<=25; i++)
        printf("imap[%d]==%d\n", i, imap[i]);
}

static void *oshfs_init(struct fuse_conn_info *conn)
{
    printf("init\n");
    size_t blocknr = sizeof(block) / sizeof(block[0]);
    size_t blocksize = size / blocknr;
    //����洢bmap��imap��inode��Ŀ�
    for(int i = 0; i <= 17; i++)
        block[i] = mmap(NULL, blocksize, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    //��ʽ��inode��
    for(int i = 0; i < 32 * 1024; i++)
        inode[i] = (struct Inode *)block[2+i/(2 *1024)] + sizeof(struct Inode)*(i%(2*1024));
    memset(block[0], 0, blocksize);
    //��bmap���趨ǰ18���ѱ�ռ��
    memset(block[0], 1, 18);
    bmap = (char *)block[0];
    //��imap���趨û���κ�һ��inode��ռ��
    memset(block[1], 0, blocksize);
    imap = (char *)block[1];
    printf("NCSIZE:%d, NCSIZEture:%d", NCSIZE, 32*1024-sizeof(struct NormalBlock *)-sizeof(int32_t));
    printf("init end\n");
    return NULL;
}

static int oshfs_getattr(const char *path, struct stat *stbuf)
{
    printf("getattr:%s\n", path);
    int ret = 0;
    int inodeID = get_inode(path);
    if(strcmp(path, "/") == 0) {
        memset(stbuf, 0, sizeof(struct stat));
        stbuf->st_mode = S_IFDIR | 0755;
    } else if(inodeID!=-1) {
        memcpy(stbuf, &fblock[inode[inodeID]->firstblockID]->st, sizeof(struct stat));
    } else {
        ret = -ENOENT;
    }
    printf("getattr end\n");
    return ret;
}

static int oshfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{
    printf("readdir\n");
    size_t blocknr = sizeof(block) / sizeof(block[0]);
    size_t blocksize = size / blocknr;
    size_t maxinode = blocknr;
    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);
    for(int i=0; i<maxinode; i++)
    {
        if(imap[i]==1)
        {
            filler(buf, fblock[inode[i]->firstblockID]->name, &fblock[inode[i]->firstblockID]->st, 0);
        }
    }
    printf("readdir end\n");
    return 0;
}

static int oshfs_mknod(const char *path, mode_t mode, dev_t dev)
{
    printf("mknod: %s\n", path);
    size_t blocknr = sizeof(block) / sizeof(block[0]);
    size_t blocksize = size / blocknr;
    size_t maxinode = blocknr;
    if(strlen(path)>256)
    {
        printf("lengh of path ERROR!");
        return -ENOENT;
    }                         //·�������Ƿ�Ϸ�
    struct stat st;
    st.st_mode = mode;
    st.st_uid = fuse_get_context()->uid;
    st.st_gid = fuse_get_context()->gid;
    st.st_nlink = 1;
    st.st_size = 0;
    char name[256];
    int nameint;
    //��ȡ��·���е��ļ���
    for(int i=0; i<256; i++)
        if(path[i]=='\0')
        {
            break;
        }else if(path[i]=='/')
            nameint = i;
    strcpy(name, &path[nameint+1]);
    //��ѯimap
    int inodeID;
    //output();
    for(int i=0; i<maxinode; i++)
    {
        //printf("imap[%d]==%d\n", i, imap[i]);
        if(imap[i]==0)
        {
            inodeID = i;
            break;
        }
    }
    imap[inodeID] = 1;     //imap��1��ռ��
    printf("\nnew inode:%d\n", inodeID);
    inode[inodeID]->info = FILE;
    //�����¿�
    inode[inodeID]->firstblockID = new_block();
    if(inode[inodeID]->firstblockID==-1) return -ENOENT; //�ռ䲻��
    struct FirstBlock *bnew =  fblock[inode[inodeID]->firstblockID];
    strcpy(bnew->name, name);
    memcpy(&bnew->st, &st, sizeof(struct stat));
    strcpy(bnew->path, path);
    bnew->next = NULL;
    inode[inodeID]->first_content = bnew->content;
    printf("mknod end\n");
    return 0;
}

static int oshfs_open(const char *path, struct fuse_file_info *fi)
{
    printf("Open\n");
    return 0;
}

static void bufoutput(const char *buf, int start, int size)
{
    for(int i=start; i<start + size; i++)
        printf("%c", buf[i]);
    printf("\n\n");
}

static int oshfs_write(const char *path, const char *buf, size_t size ,off_t offset, struct fuse_file_info *fi)
{
    printf("write\n");
    int inodeID = get_inode(path);
    struct Inode *node = inode[inodeID];
    fblock[node->firstblockID]->st.st_size = offset + size;
    if(offset + size < FCSIZE)
    {  //��һ�������д��
        printf("1:\n");
        memcpy(node->first_content + offset, buf, size);
        //bufoutput(buf, 0, size);
        fblock[node->firstblockID]->next = NULL;
        printf("write end1\n");
        return size;
    }else
    {
        int write = 0;					//��д����ֽ���
        int off = 0;                       //��offset���ֽ���
        //memcpy(node->first_content + offset, buf, FCSIZE - offset);
        //write += FCSIZE - offset;
        printf("cmpif!\n");
        struct NormalBlock *nnode;
        if(fblock[node->firstblockID]->next==NULL)
        {
            //�����¿�
            int newID = new_block();
            nnode = nblock[newID];
            fblock[node->firstblockID]->next = nnode;
            nnode->next = NULL;
        }else
            nnode = fblock[node->firstblockID]->next;
        if(offset>FCSIZE)
        {
            off+=FCSIZE;
            printf("ru\n");
            while(offset-off>NCSIZE)
            {
                if(nnode->next==NULL)
                {
                    //�����¿�
                    int newID = new_block();
                    nnode->next = nblock[newID];
                    nnode = nnode->next;
                    nnode->next = NULL;
                }else
                    nnode = nnode->next;
                off+=NCSIZE;
            }
            //offset��ʼ���Ѿ��ڴ�nnode�ڣ����۴˿��Ƿ�д
            if(offset-off+size<NCSIZE)
            {
                printf("zu gou");
                memcpy(nnode->content+offset-off, buf, size);
                //bufoutput(buf, 0, size);
                write += size;
                off = offset;
                return size;
            }else
            {
                printf("bu gou");
                memcpy(nnode->content+offset-off, buf, NCSIZE-(offset-off));
                //bufoutput(buf, 0, NCSIZE-(offset-off));
                write += NCSIZE-(offset-off);
                off = offset;
                if(nnode->next==NULL)
                {
                    //�����¿�
                    int newID = new_block();
                    nnode->next = nblock[newID];
                    nnode = nnode->next;
                    nnode->next = NULL;
                }else
                    nnode = nnode->next;
            }
        }
        else
        {
            printf("chu\n");
            memcpy(node->first_content + offset, buf, FCSIZE - offset);
            printf("2:\n");
            //bufoutput(buf, 0, FCSIZE - offset);
            write += FCSIZE - offset;
            off = offset;
        }
        while(size-write>NCSIZE)
        {
            printf("offset,off:%d,%d\n", offset, off);
            memcpy(nnode->content, &buf[write], NCSIZE);
            printf("3:\n");
            //bufoutput(buf, write, NCSIZE);
            write += NCSIZE;
            if(nnode->next==NULL)
            {
                //�����¿�
                int newID = new_block();
                nnode->next = nblock[newID];
                nnode = nnode->next;
                nnode->next = NULL;
            }else
                nnode = nnode->next;
        }
        printf("4:\n");
        memcpy(nnode->content, &buf[write], size-write);
        //bufoutput(buf, write, size-write);
        nnode->next = NULL;
        printf("write end2\n");
        return size;
    }
}

static int oshfs_truncate(const char *path, off_t size)
{
    printf("truncate\n");
    int inodeID = get_inode(path);
    struct Inode *node = inode[inodeID];
    fblock[node->firstblockID]->st.st_size = size;
    if(size < FCSIZE)
    {  //��һ�����㹻
        if(fblock[node->firstblockID]->next!=NULL)
        {
            release_block((void *) fblock[node->firstblockID]->next);
            fblock[node->firstblockID]->next = NULL;
        }
        printf("truncate end1\n");
        return 0;
    }else
    {
        int set = 0;					//�Ѽ�¼���ֽ���
        set += FCSIZE;
        struct NormalBlock *nnode;
        if(fblock[node->firstblockID]->next==NULL)
        {
            //�����¿�
            int newID = new_block();
            nnode = nblock[newID];
            fblock[node->firstblockID]->next = nnode;
            nnode->next = NULL;
        }else
            nnode = fblock[node->firstblockID]->next;
        while(size-set>NCSIZE)
        {
            set += NCSIZE;
            if(nnode->next==NULL)
            {
                //�����¿�
                int newID = new_block();
                nnode->next = nblock[newID];
                nnode = nnode->next;
                nnode->next = NULL;
            }else
                nnode = nnode->next;
        }
        if(nnode->next!=NULL)
        {
            release_block((void *) nnode->next);
            nnode->next = NULL;
        }
        printf("truncate end2\n");
        return 0;
    }
}

static void bread()
{
    for(int i=18; i<22; i++)
    {
        printf("\n-----------------------------------------------\n%d:\n", i);
        for(int j=0; j<32*1024; j++)
            printf("%c", *((char *)block[i] +j));
    }
    int i;
    scanf("%d", &i);
}

static int oshfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    printf("read\n");
    //bread();
    int inodeID = get_inode(path);
    struct Inode *node = inode[inodeID];
    int ret = size;
    if(node->first_content==NULL)
        return 0;
    else
    {
        int read=0;                    //�Ѷ��ֽ���
        int left=fblock[node->firstblockID]->st.st_size; //�ļ�ʣ���ֽ���
        int off=0;                          //�Ѿ�offset���ֽ���
        if(offset + size <= FCSIZE)
        {
            if(offset + size > left)
                ret = left - offset;
            printf("1offset:%d, off:%d, size:%d, read:%d, left:%d\n", offset, off, size, read, left);
            memcpy(buf, node->first_content + offset, ret);
            printf("read end1\n");
            return ret;
        }else
        {
            if(fblock[node->firstblockID]->next==NULL)
            {
                if(offset>FCSIZE)
                {
                    printf("2offset:%d, off:%d, size:%d, read:%d, left:%d\n", offset, off, size, read, left);
                    return 0;
                }
                printf("3offset:%d, off:%d, size:%d, read:%d, left:%d\n", offset, off, size, read, left);
                ret = left - offset;
                memcpy(buf, node->first_content + offset, ret);
                printf("read end2\n");
                return ret;
            }
            //memcpy(buf, node->first_content + offset, FCSIZE-offset);
            //read = FCSIZE - offset;
            //left -= FCSIZE - offset;
            //���϶��ǿ��ǵ���һ����������������������ͨ���ݿ����
            struct NormalBlock *nnode = fblock[node->firstblockID]->next;
            if(offset>FCSIZE)
            {
                off += FCSIZE;
                left -= FCSIZE;
                while(offset-off>NCSIZE)
                {
                    if(nnode->next!=NULL)
                    {
                        off += NCSIZE;
                        left -= NCSIZE;
                        nnode = nnode->next;
                    }else
                    {
                        printf("4offset:%d, off:%d, size:%d, read:%d, left:%d\n", offset, off, size, read, left);
                        return 0;
                    }
                }
                if(left<offset-off)
                {
                    printf("5offset:%d, off:%d, size:%d, read:%d, left:%d\n", offset, off, size, read, left);
                    return 0;
                }
                else
                {
                    if(left>NCSIZE)
                    {
                        if(offset-off+size<=NCSIZE)
                        {
                            printf("off1\n");
                            printf("6offset:%d, off:%d, size:%d, read:%d, left:%d\n", offset, off, size, read, left);
                            memcpy(&buf[read], nnode->content+(offset-off), size);
                            read+= size;
                            left-= size;
                            return size;
                        }else
                        {
                            printf("off2\n");
                            printf("7offset:%d, off:%d, size:%d, read:%d, left:%d\n", offset, off, size, read, left);
                            memcpy(&buf[read], nnode->content+(offset-off), NCSIZE-(offset-off));
                            read+= NCSIZE-(offset-off);
                            left-= NCSIZE;
                            off = offset;
                            nnode = nnode->next;
                        }
                    }else
                    {
                        if(offset-off+size<=left)
                        {
                            printf("off3\n");
                            printf("8offset:%d, off:%d, size:%d, read:%d, left:%d\n", offset, off, size, read, left);
                            memcpy(&buf[read], nnode->content+(offset-off), size);
                            read+= size;
                            left-= size;
                            return size;
                        }else
                        {
                            printf("off4\n");
                            printf("9offset:%d, off:%d, size:%d, read:%d, left:%d\n", offset, off, size, read, left);
                            memcpy(&buf[read], nnode->content+(offset-off), left-(offset-off));
                            return left-(offset-off);
                        }
                    }
                }
            }else
            {
                printf("10offset:%d, off:%d, size:%d, read:%d, left:%d\n", offset, off, size, read, left);
                memcpy(buf, node->first_content + offset, FCSIZE-offset);
                read += FCSIZE - offset;
                left -= FCSIZE;
            }
            // �����Ǵ���offset��������ʼλ��
            printf("start:\n");
            printf("num:%d\n", nnode->num);
            while(nnode->next!=NULL&&size-read>NCSIZE)
            {				//��������һ���ļ���
                printf("0\n");
                printf("11offset:%d, off:%d, size:%d, read:%d, left:%d\n", offset, off, size, read, left);
                memcpy(&buf[read], nnode->content, NCSIZE);
                read += NCSIZE;
                left -= NCSIZE;
                nnode = nnode->next;
            }
            //��ֹ�����1.���һ�� 2.ʣ������ڱ����ڽ��� 3.���߽��
            printf("final:\n");
            if(nnode->next==NULL)
            {
                if(left<size-read)
                {			//1
                    printf("1\n");
                    printf("12offset:%d, off:%d, size:%d, read:%d, left:%d\n", offset, off, size, read, left);
                    memcpy(&buf[read], nnode->content, left);
                    read += left;
                    left -= left;
                    ret -= size-read;
                    printf("read end3\n");
                    return ret;
                }else
                {			//3
                    printf("3\n");
                    printf("13offset:%d, off:%d, size:%d, read:%d, left:%d\n", offset, off, size, read, left);
                    memcpy(&buf[read], nnode->content, size-read);
                    printf("read end4\n");
                    return ret;
                }
            }
            if(size-read<NCSIZE)
            {				//2
                printf("2\n");
                printf("14offset:%d, off:%d, size:%d, read:%d, left:%d\n", offset, off, size, read, left);
                memcpy(&buf[read], nnode->content, size-read);
                printf("read end5\n");
                return ret;
            }
        }
    }
}

static int oshfs_unlink(const char *path)
{
    printf("unlink: %s\n", path);
    int inodeID = get_inode(path);
    struct Inode *node = inode[inodeID];
    release_block((void *)fblock[node->firstblockID]);
    imap[inodeID] = 0;
    printf("unlink end\n");
    return 0;
}

static const struct fuse_operations op = {
        .init = oshfs_init,
        .getattr = oshfs_getattr,
        .readdir = oshfs_readdir,
        .mknod = oshfs_mknod,
        .open = oshfs_open,
        .write = oshfs_write,
        .truncate = oshfs_truncate,
        .read = oshfs_read,
        .unlink = oshfs_unlink,
};

int main(int argc, char *argv[])
{
    return fuse_main(argc, argv, &op, NULL);
}
