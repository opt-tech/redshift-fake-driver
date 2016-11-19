package org.h2.jdbc;

import jp.ne.opt.redshiftfake.FakeConnection;
import jp.ne.opt.redshiftfake.Global;
import jp.ne.opt.redshiftfake.s3.S3ServiceImpl;
import org.h2.Driver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class FakeH2Driver extends Driver {
    private static final String urlPrefix = "jdbc:h2redshift";

    static {
        try {
            DriverManager.registerDriver(new FakeH2Driver());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        return new FakeConnection(
                DriverManager.getConnection(url.replaceFirst(urlPrefix, "jdbc:h2"), info),
                new S3ServiceImpl(Global.s3Endpoint())
        );
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith(urlPrefix);
    }
}
