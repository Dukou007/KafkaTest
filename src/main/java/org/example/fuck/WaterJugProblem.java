package org.example.fuck;

import java.util.*;

class State {
    int a, b; // a: 7升桶的水量, b: 5升桶的水量
    String operation; // 当前操作
    State parent; // 父状态，用于追溯路径

    State(int a, int b, String operation, State parent) {
        this.a = a;
        this.b = b;
        this.operation = operation;
        this.parent = parent;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        State state = (State) o;
        return a == state.a && b == state.b;
    }

    @Override
    public int hashCode() {
        return Objects.hash(a, b);
    }
}

public class WaterJugProblem {
    private static final int A_CAPACITY = 7; // 7升桶
    private static final int B_CAPACITY = 5; // 5升桶
    private static final int TARGET = 6; // 目标水量

    public static void main(String[] args) {
        solve();
    }

    private static void solve() {
        Queue<State> queue = new LinkedList<>();
        Set<State> visited = new HashSet<>();
        State initial = new State(0, 0, "Initial state", null);
        queue.add(initial);
        visited.add(initial);

        while (!queue.isEmpty()) {
            State current = queue.poll();

            // 检查是否达到目标（A或B中有6升）
            if (current.a == TARGET || current.b == TARGET) {
                printPath(current);
                return;
            }

            // 可能的操作
            List<State> nextStates = new ArrayList<>();

            // 1. 装满7升桶
            nextStates.add(new State(A_CAPACITY, current.b, "Fill A (7L bucket)", current));

            // 2. 装满5升桶
            nextStates.add(new State(current.a, B_CAPACITY, "Fill B (5L bucket)", current));

            // 3. 清空7升桶
            nextStates.add(new State(0, current.b, "Empty A (7L bucket)", current));

            // 4. 清空5升桶
            nextStates.add(new State(current.a, 0, "Empty B (5L bucket)", current));

            // 5. 从7升桶倒水到5升桶
            int pourAToB = Math.min(current.a, B_CAPACITY - current.b);
            nextStates.add(new State(current.a - pourAToB, current.b + pourAToB,
                    "Pour A to B (7L to 5L)", current));

            // 6. 从5升桶倒水到7升桶
            int pourBToA = Math.min(current.b, A_CAPACITY - current.a);
            nextStates.add(new State(current.a + pourBToA, current.b - pourBToA,
                    "Pour B to A (5L to 7L)", current));

            // 将合法的新状态加入队列
            for (State next : nextStates) {
                if (!visited.contains(next)) {
                    queue.add(next);
                    visited.add(next);
                }
            }
        }

        System.out.println("No solution found.");
    }

    private static void printPath(State state) {
        List<State> path = new ArrayList<>();
        State current = state;
        while (current != null) {
            path.add(current);
            current = current.parent;
        }
        Collections.reverse(path);

        System.out.println("Steps to achieve " + TARGET + " liters:");
        int step = 0;
        for (State s : path) {
            System.out.println("Step " + step + ": " + s.operation + " -> A=" + s.a + "L, B=" + s.b + "L");
            step++;
        }
    }
}