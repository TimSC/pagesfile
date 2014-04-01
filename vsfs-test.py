
import vsfs, os

if __name__=="__main__":
	fs = vsfs.Vsfs("test.vsfs", 1)
	fs._print_layout()
	#fs._create_inode(0, 1)
	fs._create_file("test", 7000, 0)
	fs._create_file("test2", 8000, 0)
	print fs.listdir("/")

