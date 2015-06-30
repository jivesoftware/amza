package com.jivesoftware.os.amza.service;

import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.List;

/**
 * @author jonathan.colt
 */
public class PeakDet {

    public static void main(String[] args) {
        double[] vector = new double[] {
            0, 1, 2, 3, 4, 5, 1, 2, 3, 4, 8, 7, 6, 5, 4, 3, 2, 1
        };
        new PeakDet().peakdet(vector, 1);
    }

    /*
     !PEAKDET Detect peaks in a vector
     !
     !        call PEAKDET(MAXTAB, MINTAB, N, V, DELTA) finds the local
     !        maxima and minima ("peaks") in the vector V of size N.
     !        MAXTAB and MINTAB consists of two columns. Column 1
     !        contains indices in V, and column 2 the found values.
     !
     !        call PEAKDET(MAXTAB, MINTAB, N, V, DELTA, X) replaces the
     !        indices in MAXTAB and MINTAB with the corresponding X-values.
     !
     !        A point is considered a maximum peak if it has the maximal
     !        value, and was preceded (to the left) by a value lower by
     !        DELTA.
     !
     ! Eli Billauer, 3.4.05 (http://billauer.co.il)
     ! Translated into Fortran by Brian McNoldy (http://andrew.rsmas.miami.edu/bmcnoldy)
     ! This function is released to the public domain; Any use is allowed.*/

    void peakdet(double[] vector, double triggerDelta) {
        /*
         use, intrinsic :: ieee_arithmetic
         implicit none

         integer, intent(in)            :: n
         real, intent(in)               :: v(n), delta
         real, intent(in), optional     :: x(n)
         real, intent(out), allocatable :: maxtab(:,:), mintab(:,:)
         integer                        :: nargin, lookformax, i, j, c, d
         real                           :: a, NaN, Pinf, Minf, &
         mn, mx, mnpos, mxpos, this, &
         x2(n), maxtab_tmp(n,2), mintab_tmp(n,2)

         nargin=command_argument_count()
         if (nargin < 6) then
         forall(j=1:n) x2(j)=dble(j)
         else
         x2=x
         if (size(v) /= size(x)) then
         print*,'Input vectors v and x must have same length'
         end if
         end if

         if (size((/ delta /)) > 1) then
         print*,'Input argument DELTA must be a scalar'
         end if

         if (delta <= 0) then
         print*,'Input argument DELTA must be positive'
         end if

         NaN=ieee_value(a, ieee_quiet_nan)
         Pinf=ieee_value(a, ieee_positive_inf)
         Minf=ieee_value(a, ieee_negative_inf)
         */


        double mn = Double.POSITIVE_INFINITY;
        double mx = Double.NEGATIVE_INFINITY;
        double mnpos = Double.NaN;
        double mxpos = Double.NaN;
        int lookformax = 1;

        List<Point2D.Double> maxtab_tmp = new ArrayList<>();
        List<Point2D.Double> mintab_tmp = new ArrayList<>();
        double[] x2 = vector;

        for (int i = 0; i < vector.length; i++) {
            double a = vector[i];
            if (a > mx) {
                mx = a;
                mxpos = x2[i];
            }
            if (a < mn) {
                mn = a;
                mnpos = x2[i];
            }
            if (lookformax == 1) {
                if (a < mx - triggerDelta) {
                    maxtab_tmp.add(new Point2D.Double(mxpos, i));
                    mn = a;
                    mnpos = x2[i];
                    lookformax = 0;
                }
            } else {
                if (a > mn + triggerDelta) {
                    mintab_tmp.add(new Point2D.Double(mnpos, i));
                    mx = a;
                    mxpos = x2[i];
                    lookformax = 1;
                }
            }
        }

        for (Point2D.Double maxtab_tmp1 : maxtab_tmp) {
            System.out.println(maxtab_tmp1);
        }

        /*
         allocate(maxtab(c,2))
         allocate(mintab(d,2))
         where (.not.ieee_is_nan(maxtab_tmp))
         maxtab=maxtab_tmp
         end where*/
    }
}
