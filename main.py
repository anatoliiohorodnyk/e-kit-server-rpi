#!/usr/bin/python3

import time
from gi.repository import GLib
from dbus.mainloop.glib import DBusGMainLoop

from utils.server.server import Server

if __name__ =='__main__':
    global main_loop
    main_loop = GLib.MainLoop()
    Server()
    main_loop.run()
    print('Success')
    time.sleep(10*60)
