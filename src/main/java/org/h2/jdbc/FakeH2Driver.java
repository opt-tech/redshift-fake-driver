package org.h2.jdbc;

import jp.ne.opt.redshiftfake.FakeConnection;
import jp.ne.opt.redshiftfake.Global;
import jp.ne.opt.redshiftfake.s3.S3Service;
import jp.ne.opt.redshiftfake.s3.S3ServiceImpl;
import org.h2.Driver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class FakeH2Driver extends Driver {
    private static final String urlPrefix = "jdbc:h2redshift";
    private static S3Service s3Service;

    static {
        try {
            DriverManager.registerDriver(new FakeH2Driver());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void setS3Service(S3Service service) {
        s3Service = service;
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (url.startsWith(urlPrefix)) {
            String h2Url = url.replaceFirst(urlPrefix, "jdbc:h2");
            return new FakeConnection(DriverManager.getConnection(h2Url, info),
                    s3Service == null ? new S3ServiceImpl(Global.s3Endpoint()) : s3Service);
        } else {
            return null;
        }
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith(urlPrefix);
    }
}
