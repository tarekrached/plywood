module Plywood {
  interface SQLDescribeRow {
    name: string;
    sqlType: string;
  }

  function postProcessIntrospect(columns: SQLDescribeRow[]): Attributes {
    return columns.map((column: SQLDescribeRow) => {
      var name = column.name;
      var sqlType = column.sqlType.toLowerCase();
      if (sqlType.indexOf('timestamp') !== -1) {
        return new AttributeInfo({ name, type: 'TIME' });
      } else if (sqlType.indexOf("varchar(") === 0) {
        return new AttributeInfo({ name, type: 'STRING' });
      } else if (sqlType === 'bigint') {
        // ToDo: make something special for integers
        return new AttributeInfo({ name, type: 'NUMBER' });
      } else if (sqlType === "double") {
        return new AttributeInfo({ name, type: 'NUMBER' });
      } else if (sqlType === 'boolean') {
        return new AttributeInfo({ name, type: 'BOOLEAN' });
      }
      return null;
    }).filter(Boolean);
  }

  //noinspection TsLint
  export class PrestoExternal extends SQLExternal {
    static type = 'DATASET';

    static fromJS(parameters: ExternalJS, requester: Requester.PlywoodRequester<any>): PrestoExternal {
      var value: ExternalValue = External.jsToValue(parameters, requester);
      return new PrestoExternal(value);
    }

    static getSourceList(requester: Requester.PlywoodRequester<any>): Q.Promise<string[]> {
      return requester({ query: "SHOW TABLES" })
        .then((sources) => {
          if (!Array.isArray(sources)) throw new Error('invalid sources response');
          if (!sources.length) return sources;
          var key = Object.keys(sources[0])[0];
          if (!key) throw new Error('invalid sources response (no key)');
          return sources.map((s: PseudoDatum) => s[key]).sort();
        });
    }

    static getVersion(requester: Requester.PlywoodRequester<any>): Q.Promise<string> {
      throw new Error('Presto does not expose its version.');
    }

    constructor(parameters: ExternalValue) {
      super(parameters, new PrestoDialect());
      this._ensureEngine("presto");
    }

    protected getIntrospectAttributes(): Q.Promise<Attributes> {
      return this.requester({
        query: `SHOW COLUMNS FROM ${this.dialect.escapeLiteral(this.source as string)}`,
      }).then(postProcessIntrospect);
    }
  }

  External.register(PrestoExternal, 'presto');
}
