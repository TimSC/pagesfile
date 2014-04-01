
import vsfs, os

if __name__=="__main__":
	fs = vsfs.Vsfs("test.vsfs", 1)
	fs._print_layout()
	#fs._create_inode(0, 1)
	for i in range(30):
		fs._create_file("test{0}".format(i), 7000, 0)
	print fs.listdir("/")

