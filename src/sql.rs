use nom::{
    branch::alt,
    bytes::complete::{tag, take_while1},
    character::complete::{alpha1, alphanumeric1, space0, space1},
    combinator::{map, opt},
    multi::separated_list0,
    sequence::{delimited, preceded, tuple},
    IResult,
};

#[derive(Debug, PartialEq)]
pub enum SQLQuery {
    Select { columns: Vec<String>, table: String },
    Insert { table: String, values: Vec<String> },
    Update { table: String, set: Vec<(String, String)> },
    Delete { table: String },
}

fn parse_identifier(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_alphanumeric() || c == '_')(input)
}

fn parse_select(input: &str) -> IResult<&str, SQLQuery> {
    let (input, _) = tag("SELECT")(input)?;
    let (input, _) = space1(input)?;
    let (input, columns) = separated_list0(delimited(space0, tag(","), space0), parse_identifier)(input)?;
    let (input, _) = space1(input)?;
    let (input, _) = tag("FROM")(input)?;
    let (input, _) = space1(input)?;
    let (input, table) = parse_identifier(input)?;
    Ok((input, SQLQuery::Select { columns: columns.iter().map(|s| s.to_string()).collect(), table: table.to_string() }))
}

fn parse_insert(input: &str) -> IResult<&str, SQLQuery> {
    let (input, _) = tag("INSERT INTO")(input)?;
    let (input, _) = space1(input)?;
    let (input, table) = parse_identifier(input)?;
    let (input, _) = space1(input)?;
    let (input, _) = tag("VALUES")(input)?;
    let (input, _) = space0(input)?;
    let (input, values) = delimited(
        tag("("),
        separated_list0(delimited(space0, tag(","), space0), parse_identifier),
        tag(")")
    )(input)?;
    Ok((input, SQLQuery::Insert { table: table.to_string(), values: values.iter().map(|s| s.to_string()).collect() }))
}

fn parse_update(input: &str) -> IResult<&str, SQLQuery> {
    let (input, _) = tag("UPDATE")(input)?;
    let (input, _) = space1(input)?;
    let (input, table) = parse_identifier(input)?;
    let (input, _) = space1(input)?;
    let (input, _) = tag("SET")(input)?;
    let (input, _) = space1(input)?;
    let (input, set) = separated_list0(
        delimited(space0, tag(","), space0),
        tuple((parse_identifier, delimited(space0, tag("="), space0), parse_identifier))
    )(input)?;
    Ok((input, SQLQuery::Update { table: table.to_string(), set: set.iter().map(|(k, _, v)| (k.to_string(), v.to_string())).collect() }))
}

fn parse_delete(input: &str) -> IResult<&str, SQLQuery> {
    let (input, _) = tag("DELETE FROM")(input)?;
    let (input, _) = space1(input)?;
    let (input, table) = parse_identifier(input)?;
    Ok((input, SQLQuery::Delete { table: table.to_string() }))
}

pub fn parse_sql(input: &str) -> IResult<&str, SQLQuery> {
    alt((parse_select, parse_insert, parse_update, parse_delete))(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_select() {
        assert_eq!(
            parse_sql("SELECT column1, column2 FROM table"),
            Ok(("", SQLQuery::Select { columns: vec!["column1".to_string(), "column2".to_string()], table: "table".to_string() }))
        );
    }

    #[test]
    fn test_parse_insert() {
        assert_eq!(
            parse_sql("INSERT INTO table VALUES (value1, value2)"),
            Ok(("", SQLQuery::Insert { table: "table".to_string(), values: vec!["value1".to_string(), "value2".to_string()] }))
        );
    }

    #[test]
    fn test_parse_update() {
        assert_eq!(
            parse_sql("UPDATE table SET column1 = value1, column2 = value2"),
            Ok(("", SQLQuery::Update { table: "table".to_string(), set: vec![("column1".to_string(), "value1".to_string()), ("column2".to_string(), "value2".to_string())] }))
        );
    }

    #[test]
    fn test_parse_delete() {
        assert_eq!(
            parse_sql("DELETE FROM table"),
            Ok(("", SQLQuery::Delete { table: "table".to_string() }))
        );
    }
}
