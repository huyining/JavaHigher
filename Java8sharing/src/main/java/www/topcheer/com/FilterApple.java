package www.topcheer.com;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 功能描述:
 * 修订历史:
 * 版本: 1.0.0
 * 作者: huyn
 * 邮箱: huyining18@163.com
 * 日期:2018/5/2 8:34
 */
public class FilterApple {

    public interface AppleFilter {
        boolean filter(Apple apple);
    }

    public static List<Apple> findApple(List<Apple> apples, AppleFilter appleFilter) {
        List<Apple> list = new ArrayList<>();
        for (Apple apple : apples) {
            if (appleFilter.filter(apple))
                list.add(apple);
        }
        return list;
    }



    public static List<Apple> findGreenApple(List<Apple> apples) {
        List<Apple> list = new ArrayList<>();
        for (Apple apple : apples) {
            if ("green".equals(apple.getColor())) {
                list.add(apple);
            }
        }
        return list;
    }


    public static List<Apple> findApple(List<Apple> apples, String color) {

        List<Apple> list = new ArrayList<>();
        for (Apple apple : apples) {
            if (color.equals(apple.getColor())) {
                list.add(apple);
            }
        }
        return list;
    }


    public static class GreenAnd160WeightFilter implements  AppleFilter{

        @Override
        public boolean filter(Apple apple) {
            return (apple.getColor().equals("green") && apple.getWeight() >=160);
        }
    }


    public static class YellowAnd150WeightFilter implements AppleFilter{

        @Override
        public boolean filter(Apple apple) {
            return (apple.getColor().equals("yellow") && apple.getWeight() < 150);
        }
    }
    public static void main(String[] args) {
        List<Apple> list = Arrays.asList(new Apple("green", 120), new Apple("yellow", 50), new Apple("green", 170));
//        List<Apple> greenApples = findGreenApple(list);
//        assert greenApples.size() == 2;
//
//        List<Apple> greenApples = findApple(list, "green");
//        System.out.println(greenApples);
//
//        List<Apple> readApples = findApple(list, "read");
//        System.out.println(readApples);

        List<Apple> result = findApple(list, new GreenAnd160WeightFilter());
        System.out.println(result);

        List<Apple> yellowList = findApple(list, new AppleFilter() {
            @Override
            public boolean filter(Apple apple) {
                return "yellow".equals(apple.getColor());
            }
        });

        System.out.println(yellowList);
    }
}
