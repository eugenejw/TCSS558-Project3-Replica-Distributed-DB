import time
import sys
import curses

def alarmloop(stdscr):
    stdscr.addstr("How many seconds (alarm1)? ")
    curses.echo()
    alarm1 = int(stdscr.getstr())
    while (1):
        time.sleep(alarm1)
        curses.flushinp()
        stdscr.clear()
        stdscr.addstr("Alarm1\n")
        stdscr.addstr("Continue (Y/N)?[Y]:")
        doit = stdscr.getch()
        stdscr.addstr("\n")
        stdscr.addstr("Input "+chr(doit)+"\n")
        stdscr.refresh()
        if doit == ord('N') or doit == ord('n'):
            stdscr.addstr("Exiting.....\n")
            break

curses.wrapper(alarmloop)
