module Plywood {
  export class PrestoDialect extends SQLDialect {
    static TIME_BUCKETING: Lookup<string> = {
      "PT1S": "second",
      "PT1M": "minute",
      "PT1H": "hour",
      "P1D":  "day",
      "P1W":  "week",
      "P1M":  "month",
      "P3M":  "quarter",
      "P1Y":  "year"
    };

    static TIME_PART_TO_FUNCTION: Lookup<string> = {
      SECOND_OF_MINUTE: 'SECOND($$)',
      SECOND_OF_HOUR: '(MINUTE($$)*60+SECOND($$))',
      SECOND_OF_DAY: '((HOUR($$)*60+MINUTE($$))*60+SECOND($$))',
      SECOND_OF_WEEK: '(((DAY_OF_WEEK($$)*24)+HOUR($$)*60+MINUTE($$))*60+SECOND($$))',
      SECOND_OF_MONTH: '((((DAY_OF_MONTH($$)-1)*24)+HOUR($$)*60+MINUTE($$))*60+SECOND($$))',
      SECOND_OF_YEAR: '((((DAY_OF_YEAR($$)-1)*24)+HOUR($$)*60+MINUTE($$))*60+SECOND($$))',

      MINUTE_OF_HOUR: 'MINUTE($$)',
      MINUTE_OF_DAY: 'HOUR($$)*60+MINUTE($$)',
      MINUTE_OF_WEEK: '(DAY_OF_WEEK($$)*24)+HOUR($$)*60+MINUTE($$)',
      MINUTE_OF_MONTH: '((DAY_OF_MONTH($$)-1)*24)+HOUR($$)*60+MINUTE($$)',
      MINUTE_OF_YEAR: '((DAY_OF_YEAR($$)-1)*24)+HOUR($$)*60+MINUTE($$)',

      HOUR_OF_DAY: 'HOUR($$)',
      HOUR_OF_WEEK: '(DAY_OF_WEEK($$)*24+HOUR($$))',
      HOUR_OF_MONTH: '((DAY_OF_MONTH($$)-1)*24+HOUR($$))',
      HOUR_OF_YEAR: '((DAY_OF_YEAR($$)-1)*24+HOUR($$))',

      DAY_OF_WEEK: '(DAY_OF_WEEK($$)+1)',
      DAY_OF_MONTH: 'DAY_OF_MONTH($$)',
      DAY_OF_YEAR: 'DAY_OF_YEAR($$)',

      WEEK_OF_MONTH: null,
      WEEK_OF_YEAR: 'WEEK($$)', // ToDo: look into mode (https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_week)

      MONTH_OF_YEAR: 'MONTH($$)',
      YEAR: 'YEAR($$)'
    };

    constructor() {
      super();
    }

    public constantGroupBy(): string {
      return "GROUP BY ''";
    }

    public timeToSQL(date: Date): string {
      if (!date) return 'NULL';
      return `TIMESTAMP '${this.dateToSQLDateString(date)}'`;
    }

    public concatExpression(a: string, b: string): string {
      return `CONCAT(${a},${b})`;
    }

    public containsExpression(a: string, b: string): string {
      return `POSITION(${a} IN ${b})>0`;
    }

    public lengthExpression(a: string): string {
      return `LENGTH(${a})`;
    }

    public regexpExpression(expression: string, regexp: string): string {
      return `(${expression} REGEXP_LIKE '${regexp}')`; // ToDo: escape this.regexp
    }

    public utcToWalltime(operand: string, timezone: Timezone): string {
      if (timezone.isUTC()) return operand;
      return `(${operand} AT TIME ZONE 'UTC' AT TIME ZONE '${timezone}')`;
    }

    public walltimeToUTC(operand: string, timezone: Timezone): string {
      if (timezone.isUTC()) return operand;
      return `(${operand} AT TIME ZONE '${timezone}' AT TIME ZONE 'UTC')`;
    }

    public timeFloorExpression(operand: string, duration: Duration, timezone: Timezone): string {
      var bucketFormat = PrestoDialect.TIME_BUCKETING[duration.toString()];
      if (!bucketFormat) throw new Error(`unsupported duration '${duration}'`);
      return this.walltimeToUTC(`DATE_TRUNC('${bucketFormat}',${this.utcToWalltime(operand, timezone)})`, timezone);
    }

    public timeBucketExpression(operand: string, duration: Duration, timezone: Timezone): string {
      return this.timeFloorExpression(operand, duration, timezone);
    }

    public timePartExpression(operand: string, part: string, timezone: Timezone): string {
      var timePartFunction = PrestoDialect.TIME_PART_TO_FUNCTION[part];
      if (!timePartFunction) throw new Error(`unsupported part ${part} in MySQL dialect`);
      return timePartFunction.replace(/\$\$/g, this.utcToWalltime(operand, timezone));
    }

    public timeShiftExpression(operand: string, duration: Duration, timezone: Timezone): string {
      // https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_date-add
      var sqlFn = "DATE_ADD("; //warpDirection > 0 ? "DATE_ADD(" : "DATE_SUB(";
      var spans = duration.valueOf();
      if (spans.week) {
        return sqlFn + operand + ", INTERVAL " + String(spans.week) + ' WEEK)';
      }
      if (spans.year) {
        operand = sqlFn + operand + ", INTERVAL '" + String(spans.year) + "' YEAR)";
      }
      if (spans.month) {
        operand = sqlFn + operand + ", INTERVAL '" + String(spans.month) + "' MONTH)";
      }
      if (spans.day) {
        operand = sqlFn + operand + ", INTERVAL '" + String(spans.day) + "' DAY)";
      }
      if (spans.hour) {
        operand = sqlFn + operand + ", INTERVAL '" + String(spans.hour) + "' HOUR)";
      }
      if (spans.minute) {
        operand = sqlFn + operand + ", INTERVAL '" + String(spans.minute) + "' MINUTE)";
      }
      if (spans.second) {
        operand = sqlFn + operand + ", INTERVAL '" + String(spans.second) + "' SECOND)";
      }
      return operand
    }

    public extractExpression(operand: string, regexp: string): string {
      return `REGEXP_EXTRACT(${operand}, '${regexp}')`;
    }

  }

}
