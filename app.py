import os
import sys
import uuid
import Tkinter as tk
from tkFileDialog import askdirectory
import json
from subprocess import *

class Application(tk.Frame):
    def __init__(self, parent, *args, **kwargs):
        tk.Frame.__init__(self, parent, *args, **kwargs)
        self.parent = parent
        root.geometry("{}x{}".format(900, 200))
        root.bind('<Return>', self.start)
        root.title("p2p Distributed FileSystem")
        # Vars
        self.uid = tk.StringVar()
        self.uid.set(uid)
        self.pwd = tk.StringVar()
        self.pwd.set(pwd)
        self.mnt = tk.StringVar()
        self.mnt.set(mnt)
        self.trk = tk.StringVar()
        self.trk.set(trk)
        self.pwd_var = tk.BooleanVar()
        self.pwd_var.set(False)
        ################# DECLARER #################
        # Frames
        self.row_uid = tk.Frame(root)
        self.row_pwd = tk.Frame(root)
        self.row_mnt = tk.Frame(root)
        self.row_trk = tk.Frame(root)
        self.row_btn = tk.Frame(root)
        # Labels
        self.lbl_uid = tk.Label(self.row_uid, width=15, text="User ID", anchor='w')
        self.lbl_pwd = tk.Label(self.row_pwd, width=15, text="Password", anchor='w')
        self.lbl_mnt = tk.Label(self.row_mnt, width=15, text="MountPoint", anchor='w')
        self.lbl_trk = tk.Label(self.row_trk, width=15, text="Tracker Address", anchor='w')
        # Entries
        self.ent_uid = tk.Entry(self.row_uid,textvariable=self.uid,width=65)
        self.ent_pwd = tk.Entry(self.row_pwd,textvariable=self.pwd,width=65, show="*")
        self.ent_mnt = tk.Entry(self.row_mnt,textvariable=self.mnt,width=65)
        self.ent_trk = tk.Entry(self.row_trk,textvariable=self.trk,width=65)
        # Buttons
        self.btn_gen = tk.Button(self.row_uid, text="Generate User", command=self.gen)
        self.btn_pwd = tk.Checkbutton(self.row_pwd, text="Show Password", variable=self.pwd_var, command=self.toggler)
        self.btn_mnt = tk.Button(self.row_mnt, text="Browse File", command=self.browse)
        self.btn_dmp = tk.Button(self.row_trk, text="Dump", command=self.dump,width=10)
        self.btn_hlp = tk.Button(self.row_btn, text="Help", command=self.help,width=10)
        self.btn_run = tk.Button(self.row_btn, text="Run", command=self.start,width=10)
        self.btn_end = tk.Button(self.row_btn, text="Exit", command=self.exit,width=10)
        ################# PACKER #################
        # Frames
        self.row_uid.pack(side=tk.TOP, fill=tk.X, padx=5, pady=5)
        self.row_pwd.pack(side=tk.TOP, fill=tk.X, padx=5, pady=5)
        self.row_mnt.pack(side=tk.TOP, fill=tk.X, padx=5, pady=5)
        self.row_trk.pack(side=tk.TOP, fill=tk.X, padx=5, pady=5)
        self.row_btn.pack(side=tk.TOP, fill=tk.X, padx=5, pady=5)
        # Labels
        self.lbl_uid.pack(side=tk.LEFT)
        self.lbl_pwd.pack(side=tk.LEFT)
        self.lbl_mnt.pack(side=tk.LEFT)
        self.lbl_trk.pack(side=tk.LEFT)
        # Entries
        self.ent_uid.pack(side=tk.LEFT, fill=tk.X)
        self.ent_pwd.pack(side=tk.LEFT, fill=tk.X)
        self.ent_mnt.pack(side=tk.LEFT, fill=tk.X)
        self.ent_trk.pack(side=tk.LEFT, fill=tk.X)  
        # Buttons
        self.btn_gen.pack(side=tk.LEFT,padx=40)
        self.btn_pwd.pack(side=tk.LEFT,padx=40)
        self.btn_mnt.pack(side=tk.LEFT,padx=40)
        self.btn_dmp.pack(side=tk.LEFT,padx=40)
        self.btn_hlp.pack(side=tk.LEFT,padx=40)
        self.btn_run.pack(side=tk.LEFT,padx=150)
        self.btn_end.pack(side=tk.LEFT)


    def browse(self):
        dirname = askdirectory()
        self.mnt.set(dirname)
        self.ent_mnt.delete(0,tk.END)
        self.ent_mnt.insert(0,dirname)
    
    def toggler(self):
        if self.pwd_var.get():
            self.ent_pwd.config(show="")
        else:
            self.ent_pwd.config(show="*")

    def gen(self):
        gen_uid = str(uuid.uuid4())
        self.uid.set(gen_uid)
        self.ent_uid.delete(0,tk.END)
        self.ent_uid.insert(0,gen_uid)
        gen_pwd = str(uuid.uuid4())
        self.pwd.set(gen_pwd)
        self.ent_pwd.delete(0,tk.END)
        self.ent_pwd.insert(0,gen_pwd)

    def dump(self):
        self.app_handler.stdin.write(b'dump\n')

    def help(self):
        print "- Help:            display commands\n- Dump:            display information about the current node.\n- Quit:            quit the program."

    def start(self):
        uid = "-uid="+self.uid.get()
        pwd = "-pwd="+self.pwd.get()
        mnt = "-mnt="+self.mnt.get()
        trk = "-trk="+self.trk.get()
        try:
            os.mkdir( self.mnt.get() )
        except Exception as err:
            pass
        self.btn_run.config(state=tk.DISABLED)
        print "Booting up..."
        # dev mode
        # self.app_handler = Popen([str(os.getcwd())+"/bin/./chord", uid,pwd,mnt,trk])      #rpc commands available, quit is called via GUI indirectly or from stdin directly
        # user mode
        self.app_handler = Popen([str(os.getcwd())+"/bin/./chord", uid,pwd,mnt,trk], stdin=PIPE)    #rpc commands interfaces disabled, stdin unavailable, quit called by parent(python) process or GUI


    def exit(self):
        try:
            self.app_handler.communicate("quit")
            self.app_handler.stdin.close()
        except Exception as e:
            print "exiting without 'quit'ing: ",e
        root.destroy()
        print "Exiting"

def gen():
    return str(uuid.uuid4())

def getLocalAddr():
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    addr = s.getsockname()[0]
    s.close()
    return addr

if __name__ == "__main__":
    print 'Launching...\n'
    root = tk.Tk()
    try:
        logger = json.load(open('user.json'))
        uid = str(logger["Uid"])
        pwd = str(logger["Pwd"])
    except Exception as err:
        # print(err)
        uid = gen()
        pwd = gen()
    trk = getLocalAddr()+":1234"
    mnt = os.path.join(str(os.getcwd()),'fuse-fs')
    Application(root).pack(side="top", fill="both", expand=True)
    root.mainloop()
