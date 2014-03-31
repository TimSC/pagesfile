
import vsfs

if __name__=="__main__":
	fs = vsfs.Vsfs("test.vsfs", 1)
	fs._print_layout()
