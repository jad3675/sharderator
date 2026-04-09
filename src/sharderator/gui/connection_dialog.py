"""Connection dialog for Elasticsearch cluster authentication."""

from __future__ import annotations

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QLabel,
    QLineEdit,
    QRadioButton,
    QVBoxLayout,
    QWidget,
)

from sharderator.util.config import AppConfig, ConnectionConfig


class ConnectionDialog(QDialog):
    def __init__(self, config: ConnectionConfig, parent: QWidget | None = None):
        super().__init__(parent)
        self.setWindowTitle("Connect to Elasticsearch")
        self.setMinimumWidth(450)
        self._config = config
        self._build_ui()
        self._load_config()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)

        self._radio_apikey = QRadioButton("Cloud ID + API Key")
        self._radio_basic = QRadioButton("Host + Basic Auth")
        layout.addWidget(self._radio_apikey)
        layout.addWidget(self._radio_basic)
        self._radio_apikey.toggled.connect(self._toggle_mode)

        form = QFormLayout()
        self._cloud_id = QLineEdit()
        self._cloud_id.setPlaceholderText("deployment:base64...")
        form.addRow("Cloud ID:", self._cloud_id)

        self._host = QLineEdit()
        self._host.setPlaceholderText("https://localhost:9200")
        form.addRow("Host:", self._host)

        self._username = QLineEdit()
        form.addRow("Username:", self._username)

        self._secret = QLineEdit()
        self._secret.setEchoMode(QLineEdit.EchoMode.Password)
        self._secret_label = QLabel("API Key:")
        form.addRow(self._secret_label, self._secret)
        layout.addLayout(form)

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _toggle_mode(self) -> None:
        api_mode = self._radio_apikey.isChecked()
        self._cloud_id.setEnabled(api_mode)
        self._host.setEnabled(not api_mode)
        self._username.setEnabled(not api_mode)
        self._secret_label.setText("API Key:" if api_mode else "Password:")

    def _load_config(self) -> None:
        if self._config.use_api_key:
            self._radio_apikey.setChecked(True)
        else:
            self._radio_basic.setChecked(True)
        self._cloud_id.setText(self._config.cloud_id)
        self._host.setText(self._config.hosts[0] if self._config.hosts else "")
        self._username.setText(self._config.username)
        self._toggle_mode()

    def get_config(self) -> ConnectionConfig:
        api_mode = self._radio_apikey.isChecked()
        cfg = ConnectionConfig(
            cloud_id=self._cloud_id.text().strip() if api_mode else "",
            hosts=(
                [self._host.text().strip()]
                if not api_mode and self._host.text().strip()
                else []
            ),
            username=self._username.text().strip() if not api_mode else "",
            use_api_key=api_mode,
        )
        secret = self._secret.text().strip()
        if secret:
            key = "api_key" if api_mode else "password"
            AppConfig.store_secret(key, secret)
        return cfg
