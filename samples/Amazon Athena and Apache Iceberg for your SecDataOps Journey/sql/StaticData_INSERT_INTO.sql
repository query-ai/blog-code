INSERT INTO icy_ubuntu_ftp_logs (unix_time, operation, response_code, client_ip, file_path, size, username, message)
VALUES
    (CAST('2024-01-25 10:23:49.000000' AS timestamp), 'RMD', 250, '64:ff9b:0:0:0:0:34db:6b1a', '/home/webuser/public/contoso.org/secure/query.toml', 0, 'root', 'Directory removed successfully.'),
    (CAST('2024-01-25 08:53:19.000000' AS timestamp), 'LIST', 550, '64:ff9b:0:0:0:0:34db:6b1a', '/home/user/ftp/emails.txt', 0, 'guest', 'Failed to list directory. Directory not found'),
    (CAST('2024-02-13 10:23:49.000000' AS timestamp), 'STOR', 550, '192.241.239.40', '/home/webuser/public/widget.co/public/signups.conf', 408371, 'administrator', 'Failed to upload file. File unavailable or access denied.'),
    (CAST('2024-02-07 07:13:59.000000' AS timestamp), 'LIST', 550, '10.100.2.63', '/home/user/ftp/homepage.csv', 0, 'root', 'Failed to list directory. Directory not found.'),
    (CAST('2024-02-01 11:49:47.000000' AS timestamp), 'MKD', 550, '64:ff9b:0:0:0:0:a64:b1', '/var/vsftpd/emails.xslx', 0, 'root', 'Failed to create directory. Directory already exists.')