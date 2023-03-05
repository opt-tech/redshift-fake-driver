package jp.ne.opt.redshiftfake.postgresql;

import jp.ne.opt.redshiftfake.FakeConnection;
import jp.ne.opt.redshiftfake.Global;
import jp.ne.opt.redshiftfake.s3.S3Service;
import jp.ne.opt.redshiftfake.s3.S3ServiceImpl;
import jp.ne.opt.redshiftfake.views.SystemViews;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Set;

public class Driver extends org.postgresql.Driver {
    private static final String urlPrefix = "jdbc:redshiftpostgresql:";
    private static S3Service s3Service;

    static {
        try {
            DriverManager.registerDriver(new Driver());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (url.startsWith(urlPrefix)) {
            final String postgresUrl = url.replaceFirst(urlPrefix, "jdbc:postgresql:");
            final Connection connection = DriverManager.getConnection(postgresUrl, info);
            SystemViews.create(connection);
	    var s3 = s3Service == null ? new S3ServiceImpl(Global.s3Endpoint()) : s3Service;
            return new FakeConnection(connection, s3);
        } else {
            return null;
        }
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith(urlPrefix);
    }
}
