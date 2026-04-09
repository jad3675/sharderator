"""Entry point for Sharderator."""

import sys

from PyQt6.QtWidgets import QApplication

from sharderator.gui.main_window import MainWindow
from sharderator.util.logging import setup_logging


def main():
    setup_logging()
    app = QApplication(sys.argv)
    app.setApplicationName("Sharderator")
    app.setOrganizationName("AHEAD")
    window = MainWindow()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
