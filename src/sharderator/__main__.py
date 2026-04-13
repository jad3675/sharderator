"""Entry point for Sharderator GUI."""

import sys


def main():
    try:
        from PyQt6.QtWidgets import QApplication
    except ImportError:
        print(
            "The Sharderator GUI requires PyQt6.\n"
            "Install with: pip install 'sharderator[gui]'\n"
            "Or use the headless CLI: sharderator-cli --help"
        )
        sys.exit(1)

    from sharderator.gui.main_window import MainWindow
    from sharderator.util.logging import setup_logging

    setup_logging()
    app = QApplication(sys.argv)
    app.setApplicationName("Sharderator")
    app.setOrganizationName("AHEAD")
    window = MainWindow()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
