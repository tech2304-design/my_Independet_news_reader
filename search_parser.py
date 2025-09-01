import re
from dataclasses import dataclass
from typing import List, Union, Optional, Tuple

# AST узлы
@dataclass
class Term:
    value: str       # одно слово или фраза в кавычках (уже без кавычек)
    phrase: bool     # был ли это "phrase"

@dataclass
class NotNode:
    node: 'Node'

@dataclass
class AndNode:
    nodes: List['Node']

@dataclass
class OrNode:
    nodes: List['Node']

Node = Union[Term, NotNode, AndNode, OrNode]

OPERATORS = {"AND", "OR", "NOT"}
TOKEN_SPLIT_RE = re.compile(r'(".*?"|\S+)')  # фразы в кавычках или одиночные токены
TERM_CLEAN_RE = re.compile(r'[^0-9A-Za-zА-Яа-яЁё_\\-]+', re.UNICODE)

def sanitize_term(raw: str) -> str:
    # Удаляем окружные кавычки если они есть (для фразы отдельная логика выше)
    term = raw.strip()
    # Можно разрешить * в конце: if term.endswith('*'): ...
    term = TERM_CLEAN_RE.sub(' ', term)
    term = term.strip()
    return term.lower()

def tokenize(raw: str) -> List[str]:
    return [t for t in TOKEN_SPLIT_RE.findall(raw) if t.strip()]

def parse_user_query(raw: str) -> Optional[Node]:
    """
    Преобразует пользовательский ввод в AST.
    Синтаксис:
      - Операторы слов: AND OR NOT (регистр неважен)
      - Префиксы: +word (AND), -word (NOT), |word (OR)
      - Фразы: "multi word phrase"
      - Между простыми термами по умолчанию AND
    """
    raw = raw.strip()
    if not raw:
        return None
    tokens = tokenize(raw)

    # Преобразуем префиксы в явные операторы
    expanded: List[str] = []
    for tok in tokens:
        if tok.startswith('"') and tok.endswith('"') and len(tok) >= 2:
            expanded.append(tok)  # фраза целиком
            continue
        # Короткие префиксы
        pref = tok[0]
        body = tok[1:] if pref in ['+', '-', '|'] else tok
        if pref in ['+', '-', '|'] and not body:
            continue
        if pref == '+':
            expanded.append("AND")
            expanded.append(body)
        elif pref == '-':
            expanded.append("NOT")
            expanded.append(body)
        elif pref == '|':
            expanded.append("OR")
            expanded.append(body)
        else:
            expanded.append(tok)

    # Вставляем явные AND между последовательными термами / фразами / закрытием NOT
    normalized: List[str] = []
    prev_was_term = False
    for tok in expanded:
        upper = tok.upper()
        is_op = upper in OPERATORS
        is_term = not is_op
        if is_term and prev_was_term:
            normalized.append("AND")
        normalized.append(tok)
        prev_was_term = is_term

    # Схлопываем повтор операторов
    cleaned: List[str] = []
    prev_op = False
    for tok in normalized:
        upper = tok.upper()
        if upper in OPERATORS:
            if prev_op:
                # заменим цепочки типа AND AND на один
                continue
            cleaned.append(upper)
            prev_op = True
        else:
            cleaned.append(tok)
            prev_op = False

    # Преобразуем в префиксный AST с учётом приоритета NOT > AND > OR
    # Алгоритм: сначала обработка NOT (унарный), затем собираем AND-группы, затем OR.
    # Шаг 1: обработка NOT
    def consume_not(seq: List[str]) -> List[Node]:
        out: List[Node] = []
        i = 0
        while i < len(seq):
            tok = seq[i]
            if tok == "NOT":
                if i + 1 < len(seq):
                    term_tok = seq[i + 1]
                    node = build_term(term_tok)
                    out.append(NotNode(node))
                    i += 2
                else:
                    i += 1
            else:
                if tok in OPERATORS:
                    out.append(tok)  # оператор останется строкой временно
                else:
                    out.append(build_term(tok))
                i += 1
        return out

    def build_term(tok: str) -> Term:
        if tok.startswith('"') and tok.endswith('"') and len(tok) >= 2:
            inner = tok[1:-1]
            cleaned = sanitize_term(inner)
            return Term(cleaned, phrase=True) if cleaned else Term("", phrase=True)
        cleaned = sanitize_term(tok)
        return Term(cleaned, phrase=False) if cleaned else Term("", phrase=False)

    not_processed = consume_not(cleaned)

    # Шаг 2: собрать AND
    def consume_and(seq: List) -> List:
        out: List = []
        current: List[Node] = []
        def flush():
            nonlocal current
            if not current:
                return
            if len(current) == 1:
                out.append(current[0])
            else:
                # фильтруем пустые terms (Term.value == "")
                non_empty = [c for c in current if not isinstance(c, Term) or c.value]
                if not non_empty:
                    out.append(Term("", phrase=False))
                else:
                    out.append(AndNode(non_empty))
            current = []

        i = 0
        while i < len(seq):
            part = seq[i]
            if part == "AND":
                # просто разделитель
                i += 1
                continue
            if part == "OR":
                flush()
                out.append("OR")
                i += 1
                continue
            else:
                current.append(part)
                i += 1
        flush()
        return out

    and_processed = consume_and(not_processed)

    # Шаг 3: собрать OR
    def consume_or(seq: List) -> Node:
        groups: List[Node] = []
        current: List[Node] = []
        def flush():
            nonlocal current
            if not current:
                groups.append(Term("", phrase=False))
            elif len(current) == 1:
                groups.append(current[0])
            else:
                groups.append(AndNode([c for c in current]))
            current = []

        for item in seq:
            if item == "OR":
                flush()
            else:
                current.append(item)
        flush()
        if len(groups) == 1:
            return groups[0]
        return OrNode(groups)

    ast = consume_or(and_processed)

    # Очистим пустые Terms
    def prune(node: Node) -> Optional[Node]:
        if isinstance(node, Term):
            if not node.value:
                return None
            return node
        if isinstance(node, NotNode):
            inner = prune(node.node)
            return NotNode(inner) if inner else None
        if isinstance(node, AndNode):
            nodes = [p for n in node.nodes if (p := prune(n))]
            if not nodes:
                return None
            if len(nodes) == 1:
                return nodes[0]
            return AndNode(nodes)
        if isinstance(node, OrNode):
            nodes = [p for n in node.nodes if (p := prune(n))]
            if not nodes:
                return None
            if len(nodes) == 1:
                return nodes[0]
            return OrNode(nodes)
        return None

    ast = prune(ast)
    return ast

def build_fts_query(ast: Node) -> str:
    """
    Преобразует AST в строку FTS5 MATCH.
    Правила:
      - Term: если phrase -> "term1 term2" иначе просто term
      - NOT X -> NOT (X)
      - AND: соединяем пробелом (implicit AND)
      - OR: используем OR
    """
    def render(n: Node) -> str:
        if isinstance(n, Term):
            if n.phrase:
                return f"\"{n.value}\""
            return n.value
        if isinstance(n, NotNode):
            return f"NOT ({render(n.node)})"
        if isinstance(n, AndNode):
            return " ".join(render(c) for c in n.nodes)
        if isinstance(n, OrNode):
            return " OR ".join(render(c) for c in n.nodes)
        return ""
    return render(ast)

def build_like_sql(ast: Node, title_col="title", summary_col="summary") -> Tuple[str, List[str]]:
    """
    Строит SQL условие (без WHERE) и список параметров для fallback LIKE.
    Для AND/OR/NOT создаём рекурсивно вложенные выражения.
    """
    def esc(s: str) -> str:
        return s.replace("%", "\\%").replace("_", "\\_")

    def term_clause(t: Term) -> Tuple[str, List[str]]:
        if t.phrase:
            pattern = f"%{esc(t.value)}%"
            return f"(lower({title_col}) LIKE ? ESCAPE '\\' OR lower({summary_col}) LIKE ? ESCAPE '\\')", [pattern, pattern]
        else:
            pattern = f"%{esc(t.value)}%"
            return f"(lower({title_col}) LIKE ? ESCAPE '\\' OR lower({summary_col}) LIKE ? ESCAPE '\\')", [pattern, pattern]

    def render(n: Node) -> Tuple[str, List[str]]:
        if isinstance(n, Term):
            return term_clause(n)
        if isinstance(n, NotNode):
            inner_sql, params = render(n.node)
            return f"(NOT {inner_sql})", params
        if isinstance(n, AndNode):
            parts, all_params = [], []
            for c in n.nodes:
                sql, p = render(c)
                parts.append(sql)
                all_params.extend(p)
            return "(" + " AND ".join(parts) + ")", all_params
        if isinstance(n, OrNode):
            parts, all_params = [], []
            for c in n.nodes:
                sql, p = render(c)
                parts.append(sql)
                all_params.extend(p)
            return "(" + " OR ".join(parts) + ")", all_params
        return "(1=1)", []
    return render(ast)