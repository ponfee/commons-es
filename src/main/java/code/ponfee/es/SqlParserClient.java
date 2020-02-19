package code.ponfee.es;

import java.sql.SQLFeatureNotSupportedException;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.common.settings.Settings;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.DefaultQueryAction;
import org.nlpcn.es4sql.query.DeleteQueryAction;
import org.nlpcn.es4sql.query.ESActionFactory;
import org.nlpcn.es4sql.query.SqlElasticRequestBuilder;

/**
 * Noop Elastic Search Client for Sql parse
 * 
 * @author Ponfee
 */
public final class SqlParserClient extends AbstractClient {

    private static final SqlParserClient SINGLETON = new SqlParserClient();

    private SqlParserClient() {
        super(Settings.EMPTY, null/* new ThreadPool(Settings.EMPTY) */);
    }

    public static SqlParserClient get() {
        return SINGLETON;
    }

    @Override
    public void close() {
        // nothing-todo
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(
        Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
        // nothing-todo
    }

    public SearchRequestBuilder prepareSearch(String querySql) throws SQLFeatureNotSupportedException, SqlParseException {
        DefaultQueryAction selectAction = (DefaultQueryAction) ESActionFactory.create(this, querySql);
        return (SearchRequestBuilder) selectAction.explain().getBuilder();
    }

    public DeleteRequestBuilder prepareDelete(String deleteSql) throws SQLFeatureNotSupportedException, SqlParseException {
        DeleteQueryAction deleteAction = (DeleteQueryAction) ESActionFactory.create(this, deleteSql);
        return (DeleteRequestBuilder) deleteAction.explain().getBuilder();
    }

    public String parseSql(String sql) throws SQLFeatureNotSupportedException, SqlParseException {
        SqlElasticRequestBuilder sqlRequestBuilder = ESActionFactory.create(this, sql).explain();
        return sqlRequestBuilder.explain();
    }
}
